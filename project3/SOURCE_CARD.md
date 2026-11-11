# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Alessia Amuzu
**Date:** 09 November 2026

## 1. The source
- Source and URL: Bluesky posts via Jetstream, wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: none
- Limits I know about (rate limits, paging, reconnects): No rate limit

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:26mv55qhyfk2jjdoyig7hkmy",
  "time_us": 1794237912982683,
  "cursor": 26994208303,
  "kind": "commit",
  "commit": {
    "rev": "3mzvacr2jnx27",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3mzvacr2ct727",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-09T15:25:02.57368800Z",
      "text": "SPEAR OF THE GATE"
    },
    "cid": "bafyreifroseootjunglfxicu227wazf6tp3yeny4w7zwftmkxaf74dnc2i"
  }
}
```

## 3. Volume maths
- Measured rate (from the probe):  51.37 per second; messages: 3083   not JSON: 0  
- Live capture needed for 50,000: 973.3 seconds (16 min)
- History / backfill available? How many days?
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): run the producer for about 1 hour (aim for about 180,000 messages), far more than 50,000 in case something fails

## 4. Topic design
- Topic name: bluesky_posts       (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: did
- Why this key (ordering, spread across partitions): Beacsue all messages with the same key go to the same partition, so one account's posts stay in order (a create before its delete). There are millions of accounts, so the messages spread evenly across the 3 partitions.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander)
- What makes two records **the same thing** (the same trade, post, aircraft position)? they are the same hting when they ahve the same same did + commit.rkey
- If the same thing can arrive in several versions, which one do I keep? keep the latest version

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join:

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
