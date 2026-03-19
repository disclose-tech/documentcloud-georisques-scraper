# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json
import hashlib

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter

from .log import SilentDropItem


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item, spider):
        """Parses date from the extracted string"""

        # Publication/inspection date
        publication_dt = datetime.datetime.strptime(item["date"], "%Y-%m-%d")

        publication_time = publication_dt.strftime("%H:%M:%S UTC")
        item["datetime"] = item["date"] + " " + publication_time

        item["datetime_dcformat"] = (
            publication_dt.isoformat(timespec="microseconds") + "Z"
        )

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on the url."""

    def process_item(self, item, spider):

        path = urlparse(item["url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class FullURLPipeline:
    """"""

    def process_item(self, item, spider):

        if item["type_document"] == "Rapport d'inspection":
            item["url"] = (
                f"https://georisques.gouv.fr/webappReport/ws/installations/inspection/{item['identifiant_fichier']}"
            )
        else:
            item["url"] = (
                f"https://georisques.gouv.fr/webappReport/ws/installations/document/{item['identifiant_fichier']}"
            )

        return item


class RaisonSocialePipeline:
    """Guard against missing raison_sociale. Drop the docs silently for now."""

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        if not adapter.get("raison_sociale"):
            SilentDropItem("Missing raison_sociale")


class SelectionOnlyPipeline:

    def open_spider(self, spider):

        if spider.selection_only:

            with open("selection_installations.json", "r") as json_file:
                self.selection = json.load(json_file)

    def process_item(self, item, spider):
        if spider.selection_only:
            if item["code_aiot"] in self.selection:
                return item
            else:
                raise SilentDropItem("Installation not in selection")
        else:
            return item


class UploadLimitPipeline:
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self, spider):
        self.number_of_docs = 0

    def process_item(self, item, spider):
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class TagDepartmentsPipeline:

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        if adapter.get("code_commune_insee"):
            codes_outre_mer = ["97", "98"]
            if item["code_commune_insee"][:2] in codes_outre_mer:
                item["departments"] = item["code_commune_insee"][:3]
            else:
                item["departments"] = item["code_commune_insee"][:2]

        return item


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def count_documents(self, event_data):
        """Counts the number of documents in event_data"""
        return sum([len(event_data[x]) for x in event_data.keys()])

    def open_spider(self, spider):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)
        squarelet_logger = logging.getLogger("squarelet")
        squarelet_logger.setLevel(logging.WARNING)

        if not spider.dry_run:
            try:
                spider.logger.info("Loading event data from DocumentCloud...")
                spider.event_data = spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            # Load from json if present
            try:

                with open(spider.event_data_file, "r") as file:
                    spider.logger.info(
                        f"Loading event data from local JSON file {spider.event_data_file}..."
                    )
                    data = json.load(file)
                    spider.event_data = data
            except:
                spider.event_data = None

        if spider.event_data:
            spider.logger.info(
                f"Loaded event data ({len(spider.event_data)} installations, {self.count_documents(spider.event_data)} documents)"
            )
        else:
            spider.logger.info("No event data was loaded.")
            spider.event_data = {}

    def process_item(self, item, spider):

        data = {
            "event_data_key": item["code_aiot"] + "/" + item["identifiant_fichier"],
            "source_scraper": f"Géorisques Scraper",
            # Metadonnées fichier
            "date": item["date"],
            "datetime": item["datetime"],
            "file_id": item["identifiant_fichier"],
            "source_file_url": item["url"],
            "source_page_url": item["installation_url"],
            "source_filename": item["source_filename"],
            "category": item["type_document"],
            # Metadonnées installation
            "installation_aiot_code": item["code_aiot"],
        }

        adapter = ItemAdapter(item)
        if adapter.get("departments") and adapter.get("departments_sources"):
            data["departments"] = item["departments"]
            data["departments_sources"] = "scraper"

        # Infos pas toujours présentes
        for k, v in {
            "code_naf": "installation_naf_code",
            "siret": "installation_siret",
            "statut_seveso": "installation_seveso_status",
            "ied": "installation_ied",
            "priorite_nationale": "installation_national_priority",
            "etat_activite": "installation_activity_status",
            "regime": "installation_regime",
            "adresse": "installation_address",
            "code_postal": "installation_postal_code",
            "code_commune_insee": "installation_municipality_insee_code",
            "commune": "installation_municipality",
            "raison_sociale": "installation_name",
            "themes": "installation_topics",
        }.items():
            if adapter.get(k):
                data[v] = item[k]

        try:
            if not spider.dry_run:
                spider.client.documents.upload(
                    item["url"],
                    project=spider.target_project,
                    title=item["nom"],
                    description=f"{item['raison_sociale']} ({item['code_aiot']})",
                    source="georisques.gouv.fr",
                    publish_at=item["datetime_dcformat"],
                    language="fra",
                    access=spider.access_level,
                    data=data,
                    noindex=spider.noindex,
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)
        else:  # No upload error, add to event_data
            spider.logger.debug(f"Uploaded {item['url']} to DocumentCloud")

            if not item["code_aiot"] in spider.event_data:
                spider.event_data[item["code_aiot"]] = [item["identifiant_fichier"]]
            else:
                spider.event_data[item["code_aiot"]].append(item["identifiant_fichier"])

            # Store event data after each upload
            if spider.run_id:  # only if run from DocumentCloud's web interface
                spider.store_event_data(spider.event_data)

        return item

    def close_spider(self, spider):
        """Store event data when the spider closes."""

        if not spider.dry_run and spider.run_id:
            spider.store_event_data(spider.event_data)
            spider.logger.info(
                f"Uploaded event data ({len(spider.event_data)} installations, {self.count_documents(spider.event_data)} documents)"
            )

            if spider.upload_event_data:
                # Upload the event_data to the DocumentCloud interface
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M")
                filename = f"event_data_Georisques_{timestamp}.json"

                with open(filename, "w+") as event_data_file:
                    json.dump(spider.event_data, event_data_file)
                    spider.upload_file(event_data_file)
                spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not spider.run_id:
            with open(spider.event_data_file, "w") as file:
                json.dump(spider.event_data, file)
                spider.logger.info(
                    f"Saved file {spider.event_data_file} ({len(spider.event_data)} installations, {self.count_documents(spider.event_data)} documents)"
                )


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.items_ok = []
        self.items_with_error = []

    def process_item(self, item, spider):

        self.items_ok.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["nom"]}
            category: {item["type_document"]}
            publication_date: {item["date"]}
            source_file_url: {item["url"]}
            source_page_url: {item["installation_url"]}
            """

            return item_string

        subject = f"Géorisques Scraper {str(spider.target_years[0])}-{str(spider.target_years[-1])} (New: {len(self.items_ok)}) [{spider.run_name}]"

        errors_content = f"ERRORS ({len(self.items_with_error)})\n\n" + "\n\n".join(
            [print_item(item, error=True) for item in self.items_with_error]
        )

        ok_content = f"SCRAPED ITEMS ({len(self.items_ok)})\n\n" + "\n\n".join(
            [print_item(item) for item in self.items_ok]
        )

        start_content = f"Géorisques Scraper Addon Run {spider.run_id}"

        content = "\n\n".join([start_content, errors_content, ok_content])

        if not spider.dry_run and spider.email_report:
            spider.send_mail(subject, content)
