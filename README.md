# Qdrant data collection and embedding insert scripts

## Export Kafka Q&A from Stack Overflow

`export_kafka_qa.py` exports 1,000 Stack Overflow questions tagged
`apache-kafka`, each paired with its accepted answer, using the Stack Exchange
API v2.3. It needs only the Python 3 standard library.

```bash
python3 export_kafka_qa.py            # optional: --key YOUR_KEY, --target N
```

- Queries `/search/advanced` with `site=stackoverflow`, `tagged=apache-kafka`,
  `accepted=True`, `pagesize=100`, sorted by votes. `accepted=True` returns
  only questions with an accepted answer, so 1,000 pairs takes about 10 to 11
  requests.
- Keeps requesting pages until it has 1,000 pairs, the API reports
  `has_more=false`, the daily quota runs out, or a safety cap of 25 requests
  is reached. The output is trimmed to exactly 1,000 pairs.
- Uses a 30 second request timeout, waits out any `backoff` the API returns,
  and retries a failed page up to 3 times (waiting as long as a throttle error
  asks).
- Keeps a question only when its `accepted_answer_id` matches a nested
  answer's `answer_id`, so both bodies come from one request per page.
- Converts both bodies from HTML to plain text: entities decoded, paragraph
  breaks kept, `<pre>` code kept verbatim with its indentation, inline code
  kept as text.
- Writes a UTF-8 JSON array of `{"question": ..., "accepted_answer": ...}` to
  `kafka_questions_and_answers.txt` and prints how many records it saved.

### API filter

On its first run the script calls `/2.3/filters/create` once and saves the
filter to `kafka_api_filter.txt`. The filter builds on the API's `default`
filter, so `has_more`, `backoff` and the quota fields are always returned, and
adds `question.body`, `question.answers` and `answer.body`. To create it ahead
of time run `python3 create_kafka_filter.py`, or:

```bash
curl --compressed "https://api.stackexchange.com/2.3/filters/create?base=default&unsafe=false&include=question.body;question.answers;answer.body"
```

You can also pass a filter with `--filter` or the `SE_API_FILTER` variable.

### Why a page can give fewer than 100 pairs

`pagesize=100` caps each page at 100 **questions**, not 100 pairs. A question
is skipped when its accepted answer is missing from the nested `answers` (for
example it was deleted) or a body is empty, and a question that moves between
pages is only counted once. Without `accepted=True`, many questions on each
page have no accepted answer at all. That is why the script pages until it has
enough pairs rather than stopping after a fixed 10 requests.

Anonymous use is limited to 300 requests per IP per day; an API key from
stackapps.com raises that to 10,000.


After collection the data from stack overflow it is then organized for insert to honor BM25 and Symantic search and inserts all records into the vector DB

The query script runs in terminal and prompts user for a query.

Everything tested and working. Ready for RAG integration and testing. This repo/project is complete.
