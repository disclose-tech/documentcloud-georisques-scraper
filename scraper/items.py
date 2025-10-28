"""Models for the scraped items."""

from scrapy.item import Item, Field


class GeorisquesItem(Item):

    code_aiot = Field()

    # Document metadata
    date = Field()
    datetime = Field()
    datetime_dcformat = Field()
    identifiant_fichier = Field()
    nom = Field()
    url = Field()
    source_filename = Field()
    type_document = Field()

    # Installation metadata
    raison_sociale = Field()
    adresse = Field()
    code_postal = Field()
    code_commune_insee = Field()
    departments = Field()
    commune = Field()
    code_naf = Field()
    siret = Field()
    statut_seveso = Field()
    ied = Field()
    priorite_nationale = Field()
    etat_activite = Field()
    regime = Field()
    installation_url = Field()

    themes = Field()
