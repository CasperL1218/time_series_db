# Storage Layer: Record Format

*Author: Saanvi Ibrahimpatnam. Part of the Version 5 persistence milestone.*

This package is the first piece of on-disk storage for the stream engine.
So far it contains one thing: **`RecordCodec`, which converts a trade
(`DataPoint`) into exactly 32 bytes and back.** Every other part of the
persistence milestone builds on this.

## Why this exists

Until now everything lived in memory. A trade is evicted from a sliding
window as soon as it's too old, and nothing holds on to it, so it's gone. When
the shell exits, everything is gone. That's fine for a demo, but a real
time-series database has to:

1. keep every trade, not just the current window
2. still have them after a restart
3. never need more memory as the history grows

The only way to do that is to write trades to disk. A disk stores only
bytes: it has no idea what a Java object is. An in-memory `DataPoint` can't
be saved as it is, because it holds memory addresses (pointers to its
`symbol` string and its `side`) and JVM bookkeeping that mean nothing after
a restart. So we need an exact, agreed rule for which bytes mean what. That
rule is the **record format**, and `RecordCodec` is the code that applies it.

## What's here

| File | Purpose |
|---|---|
| [`RecordCodec.java`](RecordCodec.java) | Encodes a trade to 32 bytes and decodes it back. Also checks whether stored bytes are a valid record. |
| [`CorruptRecordException.java`](CorruptRecordException.java) | Thrown when stored bytes are damaged. It gives the byte offset and the reason. |
| [`RecordCodecTest.java`](../../../../../test/java/com/tsdb/storage/RecordCodecTest.java) | 45 checks covering the layout, round trips, damage detection and bad input |
| [`docs/STORAGE_FORMAT.md`](../../../../../../docs/STORAGE_FORMAT.md) | The byte-by-byte format specification, with a real example |

### The API

```java
ByteBuffer buf = RecordCodec.allocate(1000);       // room for 1000 trades, little-endian

RecordCodec.encode(trade, buf);                    // writes 32 bytes, moves position forward 32

DataPoint t   = RecordCodec.decode(buf, 32 * i);   // reads record i; throws CorruptRecordException if damaged
long ts       = RecordCodec.timestampAt(buf, off); // only the timestamp: fast, for binary search
boolean ok    = RecordCodec.isValid(buf, off);     // is this a complete, undamaged record? (for crash recovery)
```

## The format

| Bytes | Field | Stored as |
|---|---|---|
| 0–7 | timestamp | 64-bit integer (epoch ms) |
| 8–15 | price | the double's raw 64 bits |
| 16–19 | quantity | 32-bit integer |
| 20 | side | `0` = BUY, `1` = SELL |
| 21–27 | symbol | ASCII letters, padded with zero bytes |
| 28–31 | checksum | CRC32C of bytes 0–27 |

All numbers are little-endian (lowest byte first). See
[`docs/STORAGE_FORMAT.md`](../../../../../../docs/STORAGE_FORMAT.md) for a
worked example.

## Design choices

Each choice below lists what was decided, why, and what else was considered.

### 1. Binary, not text (CSV/JSON) or a database (SQLite)

We considered three options:

- **CSV or JSON lines.** You can read the file yourself, but a trade takes
  about 50–100 bytes instead of 32. Turning numbers into text and back is
  slow, and lines have different lengths, so you can't jump to trade number
  *N* without reading every line before it.
- **SQLite.** It would work, but it needs an outside library, and it would
  hide exactly the parts this practicum is about: how data is laid out,
  indexed and recovered.
- **Binary, chosen.** It's compact, fast, has no dependencies, and fits the
  original proposal's "append-only, time-ordered segmented storage."

### 2. Every record is exactly 32 bytes

This is the most important choice. Because every record is the same size,
**record *i* always starts at byte `32 × i`** (plus a file header). That
gives us two things for free:

- **Jump to any trade instantly.** No index file is needed to find its location.
- **Binary search by time.** Trades are written in time order, so finding
  "the first trade after 2:00 pm" takes about 20 reads for a million trades,
  not a million.

32 is also a power of two, so a record never spans two 4 KB disk pages. The
fields happen to fit in 32 bytes exactly, with nothing wasted.

### 3. Prices stored as raw bits

`Double.doubleToRawLongBits(price)` stores the price's exact 64 bits.
Decoding gives back a bit-for-bit identical value. That includes tricky ones
like `0.1 + 0.2 = 0.30000000000000004`, `-0.0` and `NaN`, which converting
to text and back can get subtly wrong. Storing whole cents as an integer was
the alternative, but it would change values the rest of the engine already
uses.

### 4. Symbols: up to 7 ASCII characters, zero-padded

Seven characters cover every US ticker, including ones like `BRK.B`, and
keep the record at 32 bytes. Unused bytes are zero, which is how decode knows
where the symbol ends. Only printable characters are allowed (no spaces, and
no letters outside ASCII), so a symbol always round-trips exactly. A symbol
that's too long is **rejected with an error, never cut short**: quietly
storing `GOOGLEX` as `GOOGLE` would corrupt data without anyone noticing.

### 5. Side stored as one byte

BUY and SELL become `0` and `1`. Using an explicit `switch` (rather than the
enum's built-in position) means reordering the Java enum can never change
what's on disk.

### 6. A checksum on every record (CRC32C)

If the computer crashes or loses power mid-write, a file can end with a
half-written or garbage record. The last 4 bytes hold a checksum calculated
from the other 28. On reading, we recompute it, and if the two don't match,
the record is damaged.

- **Why CRC32C:** it's built into Java (`java.util.zip.CRC32C`) and
  hardware-accelerated on modern CPUs, and it's guaranteed to catch every
  error within 32 consecutive bits. So **any change to a single byte is
  always detected.** The test proves this for all 8,160 possible single-byte
  changes.
- **Why on every record,** not once per file: crash recovery has to find
  exactly *where* the good data ends, record by record.

`isValid` also checks that the side byte is 0 or 1 and that the symbol bytes
are well-formed. An all-zero record, which is what a crash often leaves
behind, is never valid.

### 7. Little-endian, and enforced

Little-endian is the native byte order on Intel and Apple chips. Every
method **refuses a buffer with the wrong byte order** instead of quietly
writing bytes in the wrong order, which would be a very confusing bug.
`RecordCodec.allocate(n)` always returns the right kind.

### 8. No ID stored in the record

Each trade will have an ID number (0, 1, 2, …), but the ID isn't stored in
the record. A record's position already gives its ID: the file's first ID
plus the record's index. This works like a CPU cache, where an address's
bits tell you where data lives. Storing the ID would waste 8 bytes per trade.

### 9. Small, careful API

- **`decode` reads by absolute offset and never moves the buffer's
  position,** so readers can decode records in any order. `encode` writes
  exactly 32 bytes and touches nothing else.
- **If `encode` fails, the buffer is left unchanged.** A bad trade can't
  leave half a record behind.
- **The codec checks only what the format needs.** It stores any timestamp,
  price or quantity exactly as given. Deciding what values make sense (for
  example, no negative quantities) belongs to the code that calls it.
- **`CorruptRecordException` is unchecked.** The normal read path can't fix
  damaged data, and recovery uses `isValid` instead of catching exceptions.
- **No shared state,** so it's safe to call from several threads at once.

## Tests

`RecordCodecTest` follows the same style as the project's other test
harnesses: a `main()` that prints each check and exits with code 0 only if
everything passes.

| Area | What it checks |
|---|---|
| Exact layout | Each field is at the right byte, e.g. quantity 250 is `fa 00 00 00` and "AAPL" is `41 41 50 4c 00 00 00` |
| Round trip | 100,000 seeded generator trades encode and decode identically |
| Extreme values | Smallest and largest timestamps and quantities, `NaN`, infinity, `-0.0`, 1- and 7-character symbols |
| Damage detection | All 8,160 single-byte changes are caught by both `isValid` and `decode` |
| Crash leftovers | All-zero and half-written records are invalid; reading past the end fails safely |
| Bad input | Symbols that are too long, empty, non-ASCII, or contain a space; a missing side; the wrong byte order; a buffer that's too small |
| Buffer behavior | Encode touches only its 32 bytes; decode works at any offset and doesn't move the position |

**Run on Mac or Linux:**

```bash
rm -rf out
javac -d out $(find src/main/java src/test/java -name "*.java")
java -cp out com.tsdb.storage.RecordCodecTest
```

**Run on Windows (PowerShell):**

```powershell
Remove-Item -Recurse -Force out -ErrorAction SilentlyContinue
javac -d out (Get-ChildItem -Recurse -Filter *.java src/main/java,src/test/java).FullName
java -cp out com.tsdb.storage.RecordCodecTest
```

Expected last line: `ALL 45 RECORD CODEC TESTS PASSED`.

**Speed:** about 9 million trades per second to encode and about 10 million
per second to decode, measured on a MacBook Pro without warm-up. A million
trades take 30.5 MB.

## How the other deliverables build on this

```
 Teammate 3   Shell + demo         QUERY, STORAGE commands; saves every generated trade
                   │ uses
 Teammate 2   History queries      filters + aggregates over the trades a scan returns
                   │ uses
 Teammate 1   DiskStore            many files, write buffer, crash recovery, scan(from, to)
                   │ uses
 Saanvi       Segment (next)       one file: header + many 32-byte records
                   │ uses
 Saanvi       RecordCodec  ◄── this package: one trade <-> 32 bytes
```

- **`Segment`** (next step, Saanvi): one file of records. It uses
  `RECORD_SIZE` to compute where record *i* is, `timestampAt` to
  binary-search by time without decoding whole records, and
  `RecordCodec.allocate` for its read and write buffers.
- **`DiskStore`** (Teammate 1): its in-memory write buffer is a
  `RecordCodec.allocate(2048)` buffer that `encode` fills until it's flushed
  to disk. Crash recovery walks the last file calling `isValid` to find
  where the good data ends, then cuts off anything after it. `scan` calls
  `decode` to turn bytes back into trades.
- **History queries** (Teammate 2) never see bytes at all. They receive
  ordinary `DataPoint`s from a scan and run the existing filters and
  aggregators on them. Because the codec round-trips exactly, a query over
  stored trades gives the same answers as the same query over trades in
  memory.
- **Shell + demo** (Teammate 3) uses this only indirectly, through
  `DiskStore`.

Because of this layering, if the format ever changes (say, to allow longer
symbols), only `RecordCodec` and the file header's version number need to
change. Nothing above `Segment` knows how a trade is laid out in bytes.

## Limits and future work

- **Symbols longer than 7 characters** would need a new format version, for
  example a symbol dictionary or 64-byte records.
- **No compression.** A trade is always 32 bytes. Time-series compression
  (storing the difference between timestamps, for instance) could shrink
  that a lot, but fixed width is what makes positions computable, so it's a
  trade-off for later.
- **Each `decode` creates a new `String` for the symbol.** That's fine for
  streaming scans. If profiling ever shows it matters, a small cache of the
  six known tickers would remove it.
- **The segment file header** (magic number, version, first ID) is
  specified and built together with `Segment`.
