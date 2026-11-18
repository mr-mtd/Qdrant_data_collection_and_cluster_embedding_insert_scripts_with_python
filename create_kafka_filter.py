#!/usr/bin/env python3
"""Optional setup: create the Stack Exchange API filter used by export_kafka_qa.py.

export_kafka_qa.py creates the filter by itself on its first run, so this
script is only needed to (re)create it ahead of time, for example after
deleting a stale ``kafka_api_filter.txt``. It calls ``/2.3/filters/create``
once and writes the filter string to ``kafka_api_filter.txt``.

The filter starts from the API's ``default`` filter, which already includes
``has_more``, ``backoff``, quota fields, ``question_id``, ``accepted_answer_id``
and ``answer_id``, and adds ``question.body``, ``question.answers`` and
``answer.body``. Equivalent curl command:

    curl --compressed "https://api.stackexchange.com/2.3/filters/create?base=default&unsafe=false&include=question.body;question.answers;answer.body"
"""

from __future__ import annotations

from pathlib import Path

from export_kafka_qa import FILTER_FILE, StackExchangeError, create_filter


def main() -> None:
    try:
        api_filter = create_filter()
    except StackExchangeError as exc:
        raise SystemExit(f"Could not create filter: {exc}")

    path = Path(__file__).resolve().with_name(FILTER_FILE)
    path.write_text(api_filter + "\n", encoding="utf-8")
    print(f"Filter: {api_filter}")
    print(f"Saved to {path}")


if __name__ == "__main__":
    main()
