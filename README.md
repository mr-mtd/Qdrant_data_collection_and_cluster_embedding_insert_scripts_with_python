# Qdrant data collection and embedding insert scripts

## Export Kafka Q&A from Stack Overflow

`export_kafka_qa.py` exports up to 1,000 Stack Overflow questions tagged
`apache-kafka`, each paired with its accepted answer, using the Stack Exchange
API v2.3. It needs only the Python 3 standard library.

### 1. One-time setup: create the API filter

```bash
python3 create_kafka_filter.py
```

This calls `/2.3/filters/create` once and saves the filter string to
`se_filter.txt`. The filter includes each question's `body`,
`accepted_answer_id` and nested `answers` (with `answer_id` and `body`), so both
bodies come back in the same request. Filters never expire. The same command as
curl:

```bash
curl --compressed "https://api.stackexchange.com/2.3/filters/create?base=none&unsafe=false&include=.backoff;.has_more;.items;.quota_remaining;.quota_max;question.question_id;question.body;question.accepted_answer_id;question.answers;answer.answer_id;answer.body"
```

You can also pass the string with `--filter` or the `SE_API_FILTER` variable.

### 2. Run the export

```bash
python3 export_kafka_qa.py            # optional: --key YOUR_KEY or SE_API_KEY=...
```

- Queries `/questions` with `site=stackoverflow`, `tagged=apache-kafka`,
  `pagesize=100`, sorted by votes, one page per request, at most 10 requests.
- Stops when the API reports `has_more=false` or after 10 requests.
- Uses a 30 second request timeout and waits out any `backoff` the API returns
  before the next request.
- Keeps a question only when its `accepted_answer_id` matches a nested
  answer's `answer_id`.
- Converts both bodies from HTML to plain text: entities decoded, paragraph
  breaks kept, `<pre>` code kept verbatim with its indentation, inline code
  kept as text.
- Writes a UTF-8 JSON array of `{"question": ..., "accepted_answer": ...}` to
  `kafka_questions_and_answers.txt` and prints how many records it saved.

### Why 10 requests can give fewer than 1,000 pairs

`pagesize=100` caps each page at 100 **questions**, not 100 pairs, so 10
requests return at most 1,000 questions. A question only becomes a record if it
has an accepted answer and that answer is present in its nested `answers`.
Many questions are unanswered or the asker never accepted an answer, and an
accepted answer may have been deleted. The API can also report
`has_more=false` early, a request can fail (throttling or an exhausted quota),
and a question that shifts between pages is counted once. The saved count is
therefore usually well below 1,000.

Anonymous use is limited to 300 requests per IP per day; an API key from
stackapps.com raises that to 10,000.
