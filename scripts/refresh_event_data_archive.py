"""Moves the live event data (stored on DocumentCloud) into the archive (event_data/archive.json).

Only reads from DocumentCloud: once the refreshed archive is committed & pushed, the
next scraper run removes the archived documents from the event data stored on DocumentCloud.

Usage:
    DC_USERNAME=... DC_PASSWORD=... python scripts/refresh_event_data_archive.py --event-id 1234
    python scripts/refresh_event_data_archive.py --from-file event_data_Georisques_20261007_1200.json
"""

import argparse
import json
import os
import sys
from pathlib import Path

import requests
from documentcloud import DocumentCloud

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.event_data import (  # noqa: E402
    ARCHIVE_PATH,
    load_archive,
    merge_event_data,
    write_archive,
)

USER_AGENT = "Disclose Georisques Scraper Add-On"


class Client(DocumentCloud):
    """DocumentCloud client that also sends a user agent when logging in.

    The login requests use a new session with the default python-requests user agent,
    which is blocked by Cloudflare on accounts.muckrock.com.
    """

    def requests_retry_session(self, *args, session=None, **kwargs):
        if session is None:
            session = requests.Session()
            session.headers["User-Agent"] = USER_AGENT
        return super().requests_retry_session(*args, session=session, **kwargs)


def fetch_event_data(event_id):
    """Fetches the event data of a scheduled add-on event from DocumentCloud."""

    client = Client(os.environ["DC_USERNAME"], os.environ["DC_PASSWORD"])
    client.session.headers.update({"User-Agent": USER_AGENT})
    response = client.get(f"addon_events/{event_id}/")
    response.raise_for_status()
    return response.json()["scratch"]


def count_documents(event_data):
    return sum(len(file_ids) for file_ids in event_data.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--event-id", help="ID of the scheduled add-on event")
    source.add_argument("--from-file", help="Local event data JSON file")
    args = parser.parse_args()

    if args.event_id:
        event_data = fetch_event_data(args.event_id) or {}
    else:
        with open(args.from_file, "r") as file:
            event_data = json.load(file)

    archive = load_archive()
    archived_before = count_documents(archive)

    merge_event_data(archive, event_data)
    write_archive(archive)

    print(
        f"Archived {count_documents(event_data)} documents from event data "
        f"({count_documents(archive) - archived_before} new), "
        f"{len(archive)} installations / {count_documents(archive)} documents "
        f"in {ARCHIVE_PATH}"
    )


if __name__ == "__main__":
    main()
