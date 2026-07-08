from datetime import datetime
import json
import os
import pandas as pd
import sys
import zipfile

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import GeorisquesItem

PAGE_SIZE = 6000

CSV_ENDPOINT = "https://georisques.gouv.fr/api/v1/csv/installations_classees?page_size={page_size}&page={page}"

DOWNLOAD_FOLDER = "downloaded_files"


def add_installation_adress(item, code_aiot, df_installations):

    liste_champs_adresse = []
    for i in [1, 2, 3]:
        field_value = df_installations.loc[code_aiot][f"adresse{i}"]
        if field_value:
            liste_champs_adresse.append(
                str(df_installations.loc[code_aiot][f"adresse{i}"])
            )

    adresse_installation = " ".join(liste_champs_adresse).strip()

    if adresse_installation:
        item["adresse"] = adresse_installation

    return item


def add_installation_metadata(item, code_aiot, df_installations, rubriques_by_aiot):

    # URL

    item["installation_url"] = df_installations.loc[code_aiot]["url"]

    # Autres infos, pas toujours présentes
    for k, v in {
        "codeNaf": "code_naf",
        "numeroSiret": "siret",
        "statutSeveso": "statut_seveso",
        "ied": "ied",
        "prioriteNationale": "priorite_nationale",
        "etatActivite": "etat_activite",
        "regimeVigueur": "regime",
        "codePostal": "code_postal",
        "codeInsee": "code_commune_insee",
        "commune": "commune",
        "raisonSociale": "raison_sociale",
    }.items():
        info_value = df_installations.loc[code_aiot][k]

        if info_value:
            item[v] = info_value

    installation_themes = []
    for category in [
        "bovins",
        "porcs",
        "volailles",
        "carriere",
        "eolienne",
        "industrie",
    ]:
        if df_installations.loc[code_aiot][category] == "true":
            installation_themes.append(category)

    if installation_themes:
        item["themes"] = installation_themes

    # Rubriques ICPE de l'installation (nomenclature), dédupliquées et triées
    rubriques = rubriques_by_aiot.get(code_aiot)
    if rubriques:
        item["nomenclature_sections"] = rubriques

    item = add_installation_adress(item, code_aiot, df_installations)

    return item


class GeorisquesCSVSpider(scrapy.Spider):
    name = "georisques_csv_spider"

    upload_limit_attained = False

    time_limit_attained = False

    # Stamped at import (process start) so setup time counts toward the limit.
    start_time = datetime.now()

    def time_limit_reached(self):
        """Return True (and latch the flag) once the run exceeds its time limit.

        Single source of truth for elapsed time; callers decide how to react.
        time_limit == 0 disables the limit.
        """

        if self.time_limit and not self.time_limit_attained:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            if elapsed > self.time_limit * 60:
                self.time_limit_attained = True

        return self.time_limit_attained

    def check_time_limit(self):
        """Closes the spider automatically if it reaches a specified duration"""

        self.logger.debug(f"Checking time limit ({self.time_limit} min)")

        if self.time_limit_reached():
            raise CloseSpider(f"Closed due to time limit ({self.time_limit} minutes)")

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    async def start(self):

        page = 1
        url = CSV_ENDPOINT.format(page_size=PAGE_SIZE, page=page)
        self.logger.info("Requesting first page...")
        yield scrapy.Request(url, callback=self.parse, cb_kwargs=dict(csv_page=page))

    def parse(self, response, csv_page):

        self.check_upload_limit()
        self.check_time_limit()

        self.logger.info(f"Processing page {csv_page}...")

        # Création du dossier pour accueillir les .zip téléchargés
        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

        # Sauvegarde du .zip
        zip_path = DOWNLOAD_FOLDER + f"/georisques_csv_page_{csv_page}.zip"
        with open(zip_path, "wb") as file:
            file.write(response.body)

        # Dézippage
        extracted_folder_path = DOWNLOAD_FOLDER + f"/georisques_csv_page_{csv_page}"
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(extracted_folder_path)

        # on importe le fichier des installations
        df_installations = pd.read_csv(
            extracted_folder_path + "/" + "InstallationClassee.csv",
            sep=";",
            encoding="ISO-8859-1",
            dtype=str,
        )
        df_installations.fillna("", inplace=True)
        df_installations.set_index(keys="codeAiot", inplace=True)

        # Rubriques ICPE (nomenclature) par installation. Plusieurs lignes par
        # codeAiot (une par alinéa), donc on agrège en un ensemble trié de
        # numeroRubrique uniques.
        df_rubriques = pd.read_csv(
            extracted_folder_path + "/" + "rubriqueIC.csv",
            sep=";",
            encoding="ISO-8859-1",
            dtype=str,
        )
        df_rubriques.fillna("", inplace=True)

        sets_by_aiot = {}
        for aiot, num in zip(
            df_rubriques["codeAiot"], df_rubriques["numeroRubrique"]
        ):
            if num:
                sets_by_aiot.setdefault(aiot, set()).add(num)
        rubriques_by_aiot = {a: sorted(s) for a, s in sets_by_aiot.items()}

        if len(df_installations) > 0:

            # Rapports d'inspection
            df_docs_inspection = pd.read_csv(
                extracted_folder_path + "/" + "inspection.csv",
                sep=";",
                encoding="ISO-8859-1",
                dtype=str,
            )
            df_docs_inspection.dropna(subset=["url"], axis=0, inplace=True)
            df_docs_inspection.fillna("", inplace=True)

            for row in df_docs_inspection.itertuples():

                item = GeorisquesItem(
                    code_aiot=row.codeAiot,
                    date=row.dateInspection,
                    identifiant_fichier=row.identifiantFichier,
                    nom=row.nom,
                    url=row.url,
                    original_doc_type="Rapport d'inspection",
                )
                item = add_installation_metadata(
                    item, row.codeAiot, df_installations, rubriques_by_aiot
                )

                if row.codeAiot not in self.event_data:
                    yield item
                elif item["identifiant_fichier"] not in self.event_data[row.codeAiot]:
                    yield item

            # Documents hors inspection

            df_docs_hors_inspection = pd.read_csv(
                extracted_folder_path + "/" + "metadataFichierHorsInspection.csv",
                sep=";",
                encoding="ISO-8859-1",
                dtype=str,
            )
            df_docs_hors_inspection.dropna(subset=["url"], axis=0, inplace=True)
            df_docs_hors_inspection.fillna("", inplace=True)

            for row in df_docs_hors_inspection.itertuples():
                item = GeorisquesItem(
                    code_aiot=row.codeAiot,
                    date=row.dateDepot,
                    identifiant_fichier=row.identifiant,
                    nom=row.nom,
                    url=row.url,
                    original_doc_type=row.type,
                )

                item = add_installation_metadata(
                    item, row.codeAiot, df_installations, rubriques_by_aiot
                )

                if row.codeAiot not in self.event_data:
                    yield item
                elif item["identifiant_fichier"] not in self.event_data[row.codeAiot]:
                    yield item

        # Next page

        if len(df_installations) < PAGE_SIZE:
            self.logger.debug(f"Page {csv_page} is the last page we need to crawl.")
            crawl_next_api_page = False
        else:
            self.logger.debug("Next page will be crawled.")
            crawl_next_api_page = True

        if crawl_next_api_page:
            csv_page += 1
            next_page_url = CSV_ENDPOINT.format(page_size=PAGE_SIZE, page=csv_page)

            yield scrapy.Request(
                next_page_url, callback=self.parse, cb_kwargs=dict(csv_page=csv_page)
            )
