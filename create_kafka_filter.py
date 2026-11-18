#!/usr/bin/env python3
"""One-time setup: create the Stack Exchange API filter used by export_kafka_qa.py.

Calls ``/2.3/filters/create`` once and writes the returned filter string to
``se_filter.txt`` next to this script. Filters never expire, so this only needs
to run once; the string can also be pasted into ``--filter`` or the
``SE_API_FILTER`` environment variable.

The filter starts from ``base=none`` and includes only what the export needs:

* wrapper: ``backoff``, ``has_more``, ``items``, ``quota_remaining``, ``quota_max``
* question: ``question_id``, ``body``, ``accepted_answer_id``, ``answers``
* answer: ``answer_id``, ``body``

Equivalent one-line command (curl must decompress the gzip response):

    curl --compressed "https://api.stackexchange.com/2.3/filters/create?base=none&unsafe=false&include=.backoff;.has_more;.items;.quota_remaining;.quota_max;question.question_id;question.body;question.accepted_answer_id;question.answers;answer.answer_id;answer.body"
"""

from __future__ import annotations

from pathlib import Path

from export_kafka_qa import FILTER_FILE, StackExchangeError, get_json

INCLUDE_FIELDS = [
    ".backoff",
    ".has_more",
    ".items",
    ".quota_remaining",
    ".quota_max",
    "question.question_id",
    "question.body",
    "question.accepted_answer_id",
    "question.answers",
    "answer.answer_id",
    "answer.body",
]


def main() -> None:
    try:
        data = get_json(
            "/filters/create",
            {"base": "none", "unsafe": "false", "include": ";".join(INCLUDE_FIELDS)},
        )
    except StackExchangeError as exc:
        raise SystemExit(f"Could not create filter: {exc}")

    api_filter = data["items"][0]["filter"]
    path = Path(__file__).resolve().with_name(FILTER_FILE)
    path.write_text(api_filter + "\n", encoding="utf-8")
    print(f"Filter: {api_filter}")
    print(f"Saved to {path}")


if __name__ == "__main__":
    main()
