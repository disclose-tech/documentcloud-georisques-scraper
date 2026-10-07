"""Event data helpers, shared by the UploadPipeline and the archive refresh script.

Event data is a dict {code_aiot: [identifiant_fichier, ...]} of the documents already
uploaded, grouped by installation.

It is split in two:
- the archive (event_data/archive.json), committed in the repo and loaded from disk
- the live event data, stored on DocumentCloud, holding only the documents that are
  not archived yet (keeps the PATCH requests to DocumentCloud small)
"""

import json
from pathlib import Path

ARCHIVE_PATH = Path(__file__).parent.parent / "event_data" / "archive.json"


def load_archive(path=ARCHIVE_PATH):
    """Loads the archived event data, or {} if there is no archive."""

    if not path.exists():
        return {}

    with open(path, "r") as file:
        return json.load(file)


def merge_event_data(archive, event_data):
    """Adds the documents of event_data to archive (in place), keeping their order."""

    for code_aiot, file_ids in event_data.items():
        archived_ids = archive.setdefault(code_aiot, [])
        known = set(archived_ids)
        for file_id in file_ids:
            if file_id not in known:
                archived_ids.append(file_id)
                known.add(file_id)

    return archive


def write_archive(event_data, path=ARCHIVE_PATH):
    """Writes the archived event data sorted by code AIOT, one installation per line."""

    path.parent.mkdir(exist_ok=True)

    lines = [
        f"{json.dumps(code_aiot)}: {json.dumps(event_data[code_aiot])}"
        for code_aiot in sorted(event_data)
    ]

    with open(path, "w") as file:
        file.write("{\n" + ",\n".join(lines) + "\n}\n")
