# OCI Streaming and the Stream Producer (Part 2 of the AI Data Platform Series)

*This is the first technical post in the series that opened with [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html). That post walked through the whole TfL-bus-arrivals demo Sandi Holub and I gave at Make IT 2026 and UKOUG 2025. This one goes one layer down: the piece that gets live data into the platform in the first place — a small Python producer and an OCI Streaming topic.*

## Why a plain Python producer, not Spark

Everything downstream of this post — bronze, silver, gold — runs as a Spark Structured Streaming job on the AI Data Platform. The producer doesn't. It's a standalone Python process that polls the TfL Unified API and writes to OCI Streaming's Kafka-compatible endpoint using `confluent-kafka`.

That's a deliberate split. TfL's public API has no push/webhook mode — you poll it. Polling, deduping, and handling an unreliable third-party HTTP endpoint is exactly the kind of lightweight, stateful loop that doesn't need a cluster: a single always-on process with a small in-memory cache is enough, and it's much easier to reason about and restart than a Spark job whose only job is essentially "call `requests.get` in a loop." Spark's structured streaming engine is what the *bronze layer* uses to consume this topic — that's a different post.

## What the TfL API actually returns

`GET https://api.tfl.gov.uk/Mode/bus/Arrivals` (with `app_id`/`app_key` query params) returns the **entire live arrivals board for every bus in London** as a single JSON array — tens of thousands of predictions, one per vehicle/stop/line combination, every time you call it. There's no delta endpoint and no filtering by area in this call; you get everything, on every poll.

Each record looks roughly like:

```json
{
  "id": "...",
  "vehicleId": "LTZ1234",
  "naptanId": "490008660N",
  "stationName": "Aldgate Station",
  "lineId": "25",
  "lineName": "25",
  "direction": "outbound",
  "timeToStation": 240,
  "expectedArrival": "2026-08-10T09:15:32Z",
  "modeName": "bus"
}
```

`expectedArrival` and `timeToStation` shift on almost every poll as the prediction refines, even when nothing else about the record changed. That matters for what comes next.

## The dedupe problem

If you republish the full board every poll, you're not streaming — you're spamming the topic with near-duplicates. Most of a given `(vehicleId, naptanId, lineId, direction)` combination's records are the *same prediction*, seen again a poll later. Only genuinely new information — an actual change in ETA, a new vehicle appearing, one leaving the board — is worth a message.

The producer keeps a small **TTL + LRU cache** keyed by record `id`:

```python
class DedupeCache:
    def __init__(self, ttl_sec: int, max_ids: int):
        self.ttl_sec = ttl_sec
        self.max_ids = max_ids
        self._store: "OrderedDict[str, Tuple[str, float]]" = OrderedDict()

    def is_duplicate(self, rec_id: str, fp: str) -> bool:
        now = time.time()
        self._evict_expired(now)
        existing = self._store.get(rec_id)
        if existing is not None and existing[0] == fp:
            self._store.move_to_end(rec_id)
            self._store[rec_id] = (fp, now)
            return True
        self._store[rec_id] = (fp, now)
        self._store.move_to_end(rec_id)
        self._evict_lru()
        return False
```

Each record gets a stable **fingerprint** — a `blake2b` hash of the JSON payload with volatile, uninteresting fields (`$type`, `modified`, `created`, `lastUpdated`) stripped out first, so a field that changes on every response without carrying new information doesn't defeat the dedupe. If the fingerprint for a given `id` hasn't changed since we last saw it (within `DEDUP_TTL_SEC`, default two hours), it's a duplicate and gets dropped. If it's new or changed, it's published and the cache is updated. `DEDUP_MAX_IDS` bounds memory with plain LRU eviction on top of the TTL.

In practice this cuts a ~15-20k-record poll down to a few hundred *actually new* records most of the time — which is the difference between a topic you can reason about and one that's just noise.

## Publishing to OCI Streaming

OCI Streaming exposes a Kafka-compatible endpoint, so the producer uses `confluent-kafka`'s `Producer` directly, authenticated with SASL_SSL/PLAIN:

```python
conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_USERNAME,   # <tenancy>/<user>/<stream-pool-OCID>
    "sasl.password": KAFKA_PASSWORD,   # an OCI Auth Token, not your console password
    "message.send.max.retries": 5,
    "retry.backoff.ms": 750,
    "socket.timeout.ms": 60000,
    "queue.buffering.max.ms": 100,
    "linger.ms": 50,
    "batch.num.messages": 1000,
    "enable.idempotence": False,  # OCI Streaming doesn't always support it well
}
```

A few OCI Streaming specifics worth calling out if you've only used Kafka proper before:

- **The username is a compound string**, not just an OCI username:
  `<tenancy-name>/<username-or-email>/<stream-pool-OCID>`. Get any part of
  it wrong and you get an opaque SASL auth failure, not a helpful error.
- **The password is an OCI Auth Token**, generated per-user under
  *Identity > Users > Auth Tokens*, shown exactly once at creation time —
  not your console login password.
- **`enable.idempotence` is off on purpose.** OCI Streaming's Kafka
  compatibility layer doesn't fully support the idempotent-producer
  protocol extensions; leaving it on causes intermittent produce failures
  that are hard to diagnose. Turning it off means you trade
  exactly-once-per-partition guarantees for retriable at-least-once
  delivery, which is the right trade for this pipeline (the silver layer's
  `MERGE` upsert makes duplicate delivery harmless downstream anyway).

Records are keyed by `id` on produce, which matters once there's more than
one partition — it's what would keep all messages for the same prediction
routed consistently if the topic were ever repartitioned.

## Throughput and retries

Two independent things can go wrong, and the producer handles them differently:

1. **The TfL API call itself fails or times out.** `fetch_tfl_arrivals()`
   retries up to `TFL_RETRIES` times with exponential backoff
   (`TFL_BACKOFF_START * 2^attempt`) *within* a single poll. If all
   retries are exhausted, the outer loop backs off further — up to
   `POLL_ERROR_BACKOFF_MAX` seconds — before trying again, so a rough
   patch on TfL's side doesn't turn into a tight failure loop.
2. **The local Kafka producer's send buffer fills up** (`BufferError`),
   which happens if OCI Streaming is applying backpressure or is briefly
   unreachable. The producer does one `flush()` + retry rather than
   dropping the record.

The bigger throughput lesson: because TfL returns the *entire* London bus
board on every call, the dedupe cache isn't an optimization, it's what
makes the volume tractable at all. Skip it, and every downstream
component — the topic, the bronze layer, the silver `MERGE` — has to
absorb 30-50x more traffic than the data actually warrants, for zero
additional information.

## Running it

```bash
cp .env.example .env
# fill in TFL_APP_ID/KEY and the OCI Streaming connection details
set -a; source .env; set +a
python3 tfl_stream_producer.py
```

![OCI Streaming pool, active, with Kafka bootstrap servers visible](https://zigavaupot.github.io/blogger-ai-data-platform-series/oci-streaming-and-stream-producer/images/stream-pool-active.png)

Within the first poll or two you should see a steady stream of
`[POLL]` lines, and the stream pool's metrics should start ticking up in
the console:

![Producer running, publishing new records to the tfl-arrivals topic](https://zigavaupot.github.io/blogger-ai-data-platform-series/oci-streaming-and-stream-producer/images/producer-running-terminal.png)

![OCI Streaming pool metrics showing incoming messages](https://zigavaupot.github.io/blogger-ai-data-platform-series/oci-streaming-and-stream-producer/images/stream-pool-metrics.png)

## What's next

With records landing in `tfl-arrivals`, the next post covers the **bronze
layer**: a Spark Structured Streaming job that reads this same topic via
the Kafka API, parses the JSON payload, adds event-time partitions, and
appends everything into a Delta table — the first stop for this data once
it's inside the AI Data Platform itself.

*Related: [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html) (series intro)*
