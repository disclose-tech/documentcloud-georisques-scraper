
# DocumentCloud Add-On

[DocumentCloud Add-Ons developement documentation](https://github.com/MuckRock/documentcloud-hello-world-addon/wiki/)

# DocumentCloud Géorisques Documents Scraper

Custom Add-On developped by Disclose to scrape documents from https://www.georisques.gouv.fr.

## Event data

Event data lists the documents already uploaded, as `{code_aiot: [identifiant_fichier, ...]}`. It is split in two:

- **Archive** ([event_data/archive.json](event_data/archive.json)): committed in the repo and loaded from disk at each run.
- **Live event data**: stored on DocumentCloud, holding only the documents uploaded since the last archive refresh. It is saved every 25 uploads and when the run ends (only if it changed), to keep DocumentCloud's requests small.

To refresh the archive (when the live event data grows, e.g. yearly), run the following, then commit `event_data/archive.json`:

```bash
DC_USERNAME=... DC_PASSWORD=... python scripts/refresh_event_data_archive.py --event-id <scheduled event ID>
```

The following run removes the archived documents from the live event data.
