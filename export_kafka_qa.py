#!/usr/bin/env python3
"""Export Stack Overflow ``apache-kafka`` questions paired with their accepted answers.

Uses the Stack Exchange API v2.3 ``/questions`` endpoint with
``site=stackoverflow``, ``tagged=apache-kafka`` and ``pagesize=100``. One page
is fetched per request and at most 10 HTTP requests are made in total.

A custom API filter makes each question come back with its ``body``, its
``accepted_answer_id`` and its nested ``answers`` (each with ``answer_id`` and
``body``), so both bodies arrive in the same request and no per-question calls
are needed. Create that filter once with ``create_kafka_filter.py`` (see the
README); this script reads it from ``--filter``, the ``SE_API_FILTER``
environment variable, or the ``se_filter.txt`` file the setup script writes.

Output: ``kafka_questions_and_answers.txt``, a UTF-8 JSON array of objects that
each hold only ``question`` and ``accepted_answer`` as plain-text strings.

Why 10 requests can give fewer than 1,000 pairs
-----------------------------------------------
``pagesize=100`` caps each page at 100 *questions*, not 100 question/answer
pairs, so 10 requests return at most 1,000 questions. Only some of those become
records:

* Many questions have no accepted answer: they are unanswered, or the asker
  never clicked "accept" (common for older or abandoned questions).
* ``accepted_answer_id`` can point to an answer that is not in the nested
  ``answers`` list (for example it was deleted), so there is no body to pair.
* The API can report ``has_more=false`` before 10 pages are read, or a request
  can fail (throttling, quota exhausted), which ends the run early.
* The same question can appear on two pages if its rank shifts between
  requests; duplicates are dropped.

So the record count is "questions fetched that have a matching accepted
answer", which is usually well under 1,000.

Only the Python 3 standard library is used.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

API_ROOT = "https://api.stackexchange.com/2.3"
SITE = "stackoverflow"
TAG = "apache-kafka"
PAGE_SIZE = 100
MAX_REQUESTS = 10
REQUEST_TIMEOUT_SECONDS = 30
OUTPUT_FILE = "kafka_questions_and_answers.txt"
FILTER_FILE = "se_filter.txt"
FILTER_ENV_VAR = "SE_API_FILTER"
KEY_ENV_VAR = "SE_API_KEY"
USER_AGENT = "kafka-qa-export/1.0 (python-urllib)"


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
class StackExchangeError(RuntimeError):
    """The API returned an error response or something that is not JSON."""


def get_json(path: str, params: dict[str, object]) -> dict:
    """GET ``API_ROOT + path`` and return the decoded JSON body.

    The Stack Exchange API always gzip-compresses responses, so the body is
    decompressed when needed. API errors (HTTP 4xx/5xx with an ``error_id``)
    are raised as ``StackExchangeError`` with the API's own message.
    """
    url = f"{API_ROOT}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={"Accept-Encoding": "gzip", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
            raw = resp.read()
            encoding = resp.headers.get("Content-Encoding", "")
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        encoding = exc.headers.get("Content-Encoding", "") if exc.headers else ""
        payload = _decode(raw, encoding, fallback={})
        raise StackExchangeError(
            f"HTTP {exc.code}: {payload.get('error_name', exc.reason)} - "
            f"{payload.get('error_message', '')}".rstrip(" -")
        ) from exc
    except urllib.error.URLError as exc:
        raise StackExchangeError(f"Request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise StackExchangeError(
            f"Request timed out after {REQUEST_TIMEOUT_SECONDS} seconds"
        ) from exc

    payload = _decode(raw, encoding)
    if "error_id" in payload:
        raise StackExchangeError(
            f"{payload.get('error_name')}: {payload.get('error_message')}"
        )
    return payload


def _decode(raw: bytes, encoding: str, fallback: dict | None = None) -> dict:
    try:
        if "gzip" in encoding.lower() or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))
    except (OSError, ValueError) as exc:
        if fallback is not None:
            return fallback
        raise StackExchangeError(f"Could not decode API response: {exc}") from exc


# --------------------------------------------------------------------------- #
# HTML -> plain text
# --------------------------------------------------------------------------- #
_BLOCK_TAGS = {
    "p", "div", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6",
    "table", "hr", "dl", "details", "summary",
}
_LINE_TAGS = {"tr", "dt", "dd"}


class _HTMLToText(HTMLParser):
    """Turn Stack Overflow post HTML into readable plain text.

    * Entities are decoded (``convert_charrefs=True``).
    * Paragraphs and other blocks are separated by a blank line.
    * ``<pre>`` content is kept verbatim, so code indentation survives.
    * Inline ``<code>`` text is kept as ordinary text.
    * List items start with ``- `` (or ``1. `` in ordered lists).
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._pending_breaks = 0
        self._at_line_start = True
        self._pre_depth = 0
        self._pre_buffer: list[str] = []
        self._lists: list[list] = []  # stack of [tag, next_number]
        self._cell_index = 0

    # -- output helpers ---------------------------------------------------- #
    def _request_break(self, newlines: int) -> None:
        self._pending_breaks = max(self._pending_breaks, newlines)

    def _emit(self, text: str) -> None:
        if not text:
            return
        if self._parts and self._pending_breaks:
            self._parts[-1] = self._parts[-1].rstrip(" ")
            self._parts.append("\n" * self._pending_breaks)
            self._at_line_start = True
        self._pending_breaks = 0
        self._parts.append(text)
        self._at_line_start = text.endswith("\n")

    def _emit_inline(self, data: str) -> None:
        text = re.sub(r"\s+", " ", data)
        if self._at_line_start or self._pending_breaks or not self._parts:
            text = text.lstrip(" ")
        elif self._parts[-1].endswith(" ") and text.startswith(" "):
            text = text[1:]
        self._emit(text)

    # -- parser callbacks -------------------------------------------------- #
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._pre_depth:
            if tag == "pre":
                self._pre_depth += 1
            elif tag == "br":
                self._pre_buffer.append("\n")
            return

        if tag == "pre":
            self._pre_depth = 1
            self._pre_buffer = []
            self._request_break(2)
        elif tag in _BLOCK_TAGS:
            self._request_break(2)
        elif tag in ("ul", "ol"):
            self._request_break(1 if self._lists else 2)
            self._lists.append([tag, 1])
        elif tag == "li":
            self._request_break(1)
            indent = "  " * max(len(self._lists) - 1, 0)
            if self._lists and self._lists[-1][0] == "ol":
                marker = f"{self._lists[-1][1]}. "
                self._lists[-1][1] += 1
            else:
                marker = "- "
            self._emit(indent + marker)
        elif tag == "br":
            self._request_break(1)
            self._at_line_start = True
        elif tag in _LINE_TAGS:
            self._request_break(1)
            self._cell_index = 0
        elif tag in ("td", "th"):
            if self._cell_index:
                self._emit(" | ")
            self._cell_index += 1
        elif tag == "img":
            alt = dict(attrs).get("alt")
            if alt:
                self._emit_inline(f"[image: {alt}]")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if self._pre_depth:
            if tag != "pre":
                return
            self._pre_depth -= 1
            if self._pre_depth:
                return
            code = "".join(self._pre_buffer).strip("\n")
            self._pre_buffer = []
            self._request_break(2)
            self._emit(code)
            self._request_break(2)
            return

        if tag in _BLOCK_TAGS:
            self._request_break(2)
        elif tag in ("ul", "ol"):
            if self._lists:
                self._lists.pop()
            self._request_break(1 if self._lists else 2)
        elif tag in ("li",) or tag in _LINE_TAGS:
            self._request_break(1)

    def handle_data(self, data: str) -> None:
        if self._pre_depth:
            self._pre_buffer.append(data)
        else:
            self._emit_inline(data)

    def text(self) -> str:
        self.close()
        if self._pre_depth and self._pre_buffer:  # unclosed <pre>
            self._request_break(2)
            self._emit("".join(self._pre_buffer).strip("\n"))
        return "".join(self._parts).strip()


def html_to_text(html: str) -> str:
    parser = _HTMLToText()
    parser.feed(html or "")
    return parser.text()


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
def resolve_filter(cli_value: str | None) -> str:
    """Pick the filter from --filter, then $SE_API_FILTER, then se_filter.txt."""
    if cli_value:
        return cli_value.strip()
    if os.environ.get(FILTER_ENV_VAR):
        return os.environ[FILTER_ENV_VAR].strip()
    filter_path = Path(__file__).resolve().with_name(FILTER_FILE)
    for path in (Path(FILTER_FILE), filter_path):
        if path.is_file():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
    sys.exit(
        "No API filter found. Run `python3 create_kafka_filter.py` once to create "
        f"{FILTER_FILE}, or pass --filter / set {FILTER_ENV_VAR}."
    )


def pair_from_question(question: dict) -> dict | None:
    """Return {"question", "accepted_answer"} or None if there is no match."""
    accepted_id = question.get("accepted_answer_id")
    if accepted_id is None:
        return None
    accepted = next(
        (a for a in question.get("answers") or [] if a.get("answer_id") == accepted_id),
        None,
    )
    if accepted is None:
        return None
    question_text = html_to_text(question.get("body", ""))
    answer_text = html_to_text(accepted.get("body", ""))
    if not question_text or not answer_text:
        return None
    return {"question": question_text, "accepted_answer": answer_text}


def export(api_filter: str, api_key: str | None, output: Path) -> int:
    records: list[dict] = []
    seen_ids: set[int] = set()
    backoff_seconds = 0
    questions_seen = 0

    for page in range(1, MAX_REQUESTS + 1):
        if backoff_seconds:
            print(f"API asked for a backoff; waiting {backoff_seconds}s ...")
            time.sleep(backoff_seconds)
            backoff_seconds = 0

        params: dict[str, object] = {
            "site": SITE,
            "tagged": TAG,
            "pagesize": PAGE_SIZE,
            "page": page,
            "order": "desc",
            "sort": "votes",
            "filter": api_filter,
        }
        if api_key:
            params["key"] = api_key

        try:
            data = get_json("/questions", params)
        except StackExchangeError as exc:
            print(f"Request {page} failed, stopping early: {exc}", file=sys.stderr)
            break

        items = data.get("items") or []
        questions_seen += len(items)
        for question in items:
            qid = question.get("question_id")
            if qid in seen_ids:
                continue
            pair = pair_from_question(question)
            if pair is not None:
                seen_ids.add(qid)
                records.append(pair)

        print(
            f"Request {page}/{MAX_REQUESTS}: {len(items)} questions, "
            f"{len(records)} pairs so far"
            + (f", quota remaining {data['quota_remaining']}" if "quota_remaining" in data else "")
        )

        backoff_seconds = int(data.get("backoff") or 0)
        if not data.get("has_more"):
            print("API reports has_more=false; no more pages.")
            break

    output.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Fetched {questions_seen} questions.")
    print(f"Saved {len(records)} question/accepted-answer records to {output}")
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--filter", help="Stack Exchange API filter string")
    parser.add_argument(
        "--key",
        default=os.environ.get(KEY_ENV_VAR),
        help=f"optional API key for a higher daily quota (or set {KEY_ENV_VAR})",
    )
    parser.add_argument("--output", default=OUTPUT_FILE, help="output file path")
    args = parser.parse_args()

    export(resolve_filter(args.filter), args.key, Path(args.output))


if __name__ == "__main__":
    main()
