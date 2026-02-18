# Design Document: Block-Level Timestamp Metadata in HFile Index

## Overview

This document describes the addition of per-block timestamp metadata to the HFile block index, enabling efficient time-range filtering at the block level during scans.

**JIRA:** HBASE-XXXX
**HFile Version:** Minor version 4 (version 3 remains unchanged)
**Target Release:** HBase 2.6.4+

## Problem Statement

### Current State

HBase currently stores min/max timestamp metadata at the HFile level in the file trailer. This enables file-level filtering during scans:
- If a scan's time range doesn't overlap with a file's timestamp range, the entire file can be skipped
- This works well for filtering entire files

### The Problem

Within a single HFile, blocks may contain data from vastly different time ranges. Consider this example:

```
HFile: timestamps from 100 to 10000
  Block 1: timestamps 100-200
  Block 2: timestamps 300-400
  Block 3: timestamps 9800-10000
```

A scan requesting timestamps `[150, 250]` would:
1. Open the HFile (timestamps overlap: ✓)
2. Read ALL blocks (no block-level filtering exists)
3. Decompress ALL blocks
4. Filter cells individually

**Result:** Blocks 2 and 3 are read and decompressed unnecessarily, wasting I/O and CPU.

### Use Case

Time-bounded queries are common in HBase workloads:
- "Get data from the last hour"
- "Scan yesterday's data"
- Time-series data with retention policies

For these queries, reducing unnecessary block reads provides significant performance improvements.

## Solution Design

### High-Level Approach

Add min/max timestamp metadata to each data block entry in the block index:
- **Storage location:** Block index (already loaded in memory)
- **Granularity:** Per-block (one min/max pair per block)
- **Scope:** DATA and ENCODED_DATA blocks only (other block types don't contain timestamped cells)
- **Format version:** Increment HFile minor version from 3 to 4

### Architecture

```
┌─────────────────────────────────────────────────────┐
│ HFile v4                                            │
├─────────────────────────────────────────────────────┤
│ Data Block 1 (ts: 100-200)                          │
│ Data Block 2 (ts: 300-400)                          │
│ Data Block 3 (ts: 9800-10000)                       │
├─────────────────────────────────────────────────────┤
│ Block Index                                         │
│   Entry 1: offset=0,    size=1KB, min=100,  max=200 │
│   Entry 2: offset=1KB,  size=1KB, min=300,  max=400 │
│   Entry 3: offset=2KB,  size=1KB, min=9800, max=10K │
├─────────────────────────────────────────────────────┤
│ File Trailer (min=100, max=10000)                   │
└─────────────────────────────────────────────────────┘
```

**Scan with time range [150, 250]:**
1. Check file-level timestamps: [100, 10000] overlaps [150, 250] → continue
2. Check block 1 index: [100, 200] overlaps [150, 250] → **read block**
3. Check block 2 index: [300, 400] doesn't overlap [150, 250] → **skip**
4. Check block 3 index: [9800, 10000] doesn't overlap [150, 250] → **skip**

**Result:** Only 1 of 3 blocks is read and decompressed.

## File Format Changes

### Block Index Entry Format

#### Root Index Blocks

Root index blocks use a variable-length format with key lengths encoded via VInt.

**v3 Format (existing):**
```
[Block Offset (8)][Block Size (4)][Key Length (VInt)][Key bytes]
```

**v4 Format (new):**
```
[Block Offset (8)][Block Size (4)][Min Timestamp (8)][Max Timestamp (8)][Key Length (VInt)][Key bytes]
```

#### Non-Root Index Blocks (Leaf and Intermediate)

Non-root index blocks use a fixed-overhead format with a secondary index for
binary search over variable-length keys.

**v3 Format (existing) — entry overhead: 12 bytes:**
```
[Block Offset (8)][Block Size (4)][Key bytes]
```

**v4 Format (new) — entry overhead: 28 bytes:**
```
[Block Offset (8)][Block Size (4)][Min Timestamp (8)][Max Timestamp (8)][Key bytes]
```

Timestamps are placed between the fixed-size fields (offset + size) and the
variable-length key so that the entry overhead remains constant, which is
required by the binary search logic.

**Size increase per block index entry:** 16 bytes (2 × long)

### Backward Compatibility

#### Version Detection

**Writer:**
- Checks `minorVersion` field to determine whether to write timestamps
- v3 files: no timestamps written (12 bytes per entry)
- v4 files: timestamps written (28 bytes per entry)

**Reader:**
- Checks file trailer's minor version
- v3 files: doesn't attempt to read timestamps
- v4 files: reads timestamps after offset/size

#### Compatibility Matrix

| Writer Version | File Written | v3 Reader | v4 Reader |
|----------------|--------------|-----------|-----------|
| v3             | v3 format    | ✅ Read   | ✅ Read   |
| v4 (config=3)  | v3 format    | ✅ Read   | ✅ Read   |
| v4 (config=4)  | v4 format    | ❌ Cannot read | ✅ Read |

**Key insight:** v4-capable code can write v3 format files by setting configuration to v3. This enables safe rollback strategies.

## Implementation Details

### Component Changes

#### 1. HFileBlock.Writer (Timestamp Tracking)

**New field:**
```java
private TimeRangeTracker blockTimeRangeTracker;
```

**Tracking logic:**
```java
public void trackTimestamp(Cell cell) {
  if (blockTimeRangeTracker != null) {
    blockTimeRangeTracker.includeTimestamp(cell);
  }
}
```

- Initialize tracker when starting DATA/ENCODED_DATA blocks
- Track each cell's timestamp as it's written
- Reset tracker when block is finished
- Provide getter to access tracker before finalization

#### 2. HFileWriterImpl (Coordination)

Coordinates between block writer and index writer:

```java
// During append()
blockWriter.trackTimestamp(cell);  // Track in current block

// During finishBlock()
TimeRangeTracker tracker = blockWriter.getBlockTimeRangeTracker();
long minTs = tracker.getMin();
long maxTs = tracker.getMax();

if (getMinorVersion() >= 4) {
  dataBlockIndexWriter.addEntry(firstKey, offset, size, minTs, maxTs);
} else {
  dataBlockIndexWriter.addEntry(firstKey, offset, size);  // v3 format
}
```

**Version-aware writing ensures v3/v4 compatibility.**

#### 3. BlockIndexChunk Interface and BlockIndexChunkImpl (Storage)

**Interface additions (`BlockIndexChunk.java`):**
```java
boolean hasTimestamps();
long getBlockMinTimestamp(int i);
long getBlockMaxTimestamp(int i);
```

These allow code to access timestamp metadata without casting to
`BlockIndexChunkImpl`.

**New fields in `BlockIndexChunkImpl`:**
```java
private final List<Long> blockMinTimestamps = new ArrayList<>();
private final List<Long> blockMaxTimestamps = new ArrayList<>();
private final int minorVersion;  // Set at construction
```

**Constructor enforcement:**
```java
public BlockIndexChunkImpl(int minorVersion) {
  this.minorVersion = minorVersion;
}
```

- Version is immutable (final field)
- Timestamps stored parallel to offsets/sizes
- Version determines serialization format

**Non-root size tracking:** When entries are added with timestamps, both
`curTotalRootSize` and `curTotalNonRootEntrySize` are incremented by 16 bytes.
This ensures `getNonRootSize()` returns the correct size for the v4 non-root
format, which controls when leaf blocks are flushed to disk.

#### 4. BlockIndexWriter (Index Management)

**Constructor:**
```java
public BlockIndexWriter(..., int minorVersion) {
  this.minorVersion = minorVersion;
  this.rootChunk = createChunk();      // Uses minorVersion
  this.curInlineChunk = createChunk(); // Uses minorVersion
}
```

**All chunks created by this writer inherit the version.**

**Multi-level index timestamp propagation:**

For large HFiles with multi-level indexes (root → intermediate → leaf → data),
timestamps are aggregated upward at each level:

1. **`writeInlineBlock()`:** Before clearing `curInlineChunk`, computes the
   aggregate min/max timestamps across all entries in the leaf block. Stores
   these in `leafBlockMinTimestamp` / `leafBlockMaxTimestamp`.

2. **`blockWritten()`:** Passes the saved aggregate timestamps to
   `rootChunk.add(firstKey, offset, onDiskSize, totalNumEntries, aggMin, aggMax)`.
   This gives each root entry the timestamp range of its entire leaf subtree.

3. **`writeIntermediateLevel()`:** When breaking the root chunk into
   intermediate-level blocks, propagates per-entry timestamps from
   `currentLevel` to `curChunk`.

4. **`writeIntermediateBlock()`:** Computes aggregate min/max from `curChunk`
   entries and passes them to `parent.add()`, so each parent entry covers the
   timestamp range of its child intermediate block.

#### 5. NoOpIndexBlockEncoder (Serialization)

**Root block write logic:**
```java
if (chunk.getMinorVersion() >= MINOR_VERSION_WITH_BLOCK_TIMERANGE) {
  out.writeLong(chunk.getBlockMinTimestamp(i));
  out.writeLong(chunk.getBlockMaxTimestamp(i));
}
// v3 chunks: nothing written
```

**Non-root block write logic (`writeNonRoot`):**
```java
out.writeLong(blockIndexChunk.getBlockOffset(i));
out.writeInt(blockIndexChunk.getOnDiskDataSize(i));
if (hasTimestamps) {
  out.writeLong(blockIndexChunk.getBlockMinTimestamp(i));
  out.writeLong(blockIndexChunk.getBlockMaxTimestamp(i));
}
out.write(blockIndexChunk.getBlockKey(i));
```

The secondary index offsets already account for the extra 16 bytes per entry
(via `curTotalNonRootEntrySize`), so binary search works unchanged.

**Root block read logic:**
```java
if (minorVersion >= MINOR_VERSION_WITH_BLOCK_TIMERANGE) {
  minTs = in.readLong();
  maxTs = in.readLong();
}
// v3 files: timestamps remain as initial values
```

**Non-root block read logic (`loadDataBlockWithScanInfo` and `midkey`):**

The reader selects the correct entry overhead based on `minorVersion`:
```java
int entryOverhead = minorVersion >= MINOR_VERSION_WITH_BLOCK_TIMERANGE
    ? SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS  // 28
    : SECONDARY_INDEX_ENTRY_OVERHEAD;                  // 12
```

This overhead is passed to `locateNonRootIndexEntry()` and
`getNonRootIndexedKey()`, which use it to skip past the fixed-size fields when
locating variable-length keys. After reading offset and size from the
positioned buffer, the reader skips the two timestamp longs before the key:
```java
currentOffset = buffer.getLong();
currentOnDiskSize = buffer.getInt();
if (minorVersion >= MINOR_VERSION_WITH_BLOCK_TIMERANGE) {
  buffer.getLong(); // skip minTimestamp
  buffer.getLong(); // skip maxTimestamp
}
```

#### 5a. HFileBlockIndex — Binary Search Overloads

Three static methods in `BlockIndexReader` gained overloads with an
`int entryOverhead` parameter. The original signatures delegate with
`SECONDARY_INDEX_ENTRY_OVERHEAD` (12) for backward compatibility:

- `binarySearchNonRootIndex(Cell, ByteBuff, CellComparator, int entryOverhead)`
- `getNonRootIndexedKey(ByteBuff, int, int entryOverhead)`
- `locateNonRootIndexEntry(ByteBuff, Cell, CellComparator, int entryOverhead)`

A new constant captures the v4 overhead:
```java
static final int SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS =
    SECONDARY_INDEX_ENTRY_OVERHEAD + 2 * Bytes.SIZEOF_LONG; // 28
```

**Symmetry:** Both writer and reader check `minorVersion` for consistency.

#### 6. Scanner Integration (Filtering)

#### Infrastructure Added

**HFileScanner interface (new method):**
```java
void setTimeRange(TimeRange timeRange);
```

**HFileScannerImpl (storage):**
```java
private TimeRange timeRange;

@Override
public void setTimeRange(TimeRange timeRange) {
  this.timeRange = timeRange;
}
```

**NoOpEncodedSeeker (filtering logic):**
```java
public boolean shouldReadBlock(int blockIndex, TimeRange scanTimeRange) {
  if (blockMinTimestamps == null) {
    return true;  // v3 file: no filtering possible
  }

  long blockMin = blockMinTimestamps[blockIndex];
  long blockMax = blockMaxTimestamps[blockIndex];
  TimeRange blockRange = TimeRange.between(blockMin, blockMax + 1);

  return scanTimeRange.includesTimeRange(blockRange);
}
```

#### Integration Status

**✅ Fully Integrated:**
- ✅ HFileScanner interface accepts TimeRange
- ✅ HFileScannerImpl stores TimeRange
- ✅ NoOpEncodedSeeker has filtering logic (`shouldReadBlock`)
- ✅ Block index has timestamp metadata
- ✅ TimeRange passed from Scan → StoreScanner → HStore → StoreFileScanner → HFileScanner
- ✅ Column family-specific time ranges supported (falls back to scan-wide time range)

**Integration Flow:**
1. `StoreScanner` extracts TimeRange from Scan (per-CF or scan-wide)
2. Passes to `HStore.getScanners(... timeRange)`
3. `HStore` passes to `StoreFileScanner.getScannersForStoreFiles(... timeRange)`
4. `StoreFileScanner` calls `hfs.setTimeRange(timeRange)` on each HFileScanner
5. HFileScanner now has TimeRange available for block filtering

### Version Enforcement Architecture

To prevent version mismatches that could cause file corruption, version is enforced at construction:

```
Configuration (hfile.format.minor.version)
            ↓
    HFileWriterImpl.getMinorVersion()
            ↓
    ┌──────────────────────────────┐
    ↓                              ↓
BlockIndexWriter(version)    BlockIndexWriter(version)
    ↓                              ↓
BlockIndexChunkImpl(version)  BlockIndexChunkImpl(version)
```

**Guarantees:**
- Cannot create chunks without version
- Cannot create writers without version
- Version is immutable after construction
- Writer and reader use same version check

## Configuration

### Property

```
hfile.format.minor.version
```

### Values

- **3 (default):** Write v3 format (no block timestamps)
  - 100% backward compatible
  - Safe to rollback at any time
  - No configuration needed

- **4:** Write v4 format (with block timestamps)
  - Enables block-level filtering
  - Requires v4-capable readers
  - Must be explicitly enabled

### Example Configuration

**Enable v4 features:**
```xml
<property>
  <name>hfile.format.minor.version</name>
  <value>4</value>
  <description>
    Enable HFile v4 features including block-level timestamp metadata.
    Default is 3 for stability and rollback compatibility.
  </description>
</property>
```

### Rollback Strategy

If rollback from v4 to v3 HBase is needed:

1. **Set configuration to v3:**
   ```xml
   <property>
     <name>hfile.format.minor.version</name>
     <value>3</value>
   </property>
   ```

2. **Rolling restart cluster** to pick up configuration

3. **Major compact all tables:**
   ```bash
   echo "major_compact" | hbase shell
   ```
   This rewrites v4 files as v3 format

4. **Verify all files are v3:**
   ```bash
   hbase hfile -f /hbase/data/.../file -m | grep "Minor version"
   ```

5. **Rollback to v3 HBase** once all files are v3 format

## Testing

### Unit Tests

**TestHFileBlockTimestamp.java** (20 tests):

1. **testBlockWriterTimestampTracking** - Verifies HFileBlock.Writer tracks timestamps correctly
2. **testBlockIndexChunkTimestamps** - Verifies chunk stores/retrieves timestamps
3. **testTimestampSerialization** - Verifies serialization produces output
4. **testTimeRangeFiltering** - Verifies shouldReadBlock() logic with various time ranges
5. **testBackwardCompatibilityNoTimestamps** - Verifies v3 files work (no timestamp metadata)
6. **testEmptyTimeRangeTracker** - Verifies empty tracker has initial values
7. **testTrackerResetBetweenBlocks** - Verifies tracker resets between blocks
8. **testMinorVersionConstant** - Verifies version constants are correct
9. **testDefaultMinorVersionIsV3** - Verifies default is v3 (conservative)
10. **testExplicitV4Configuration** - Verifies v4 can be enabled via config
11. **testChunkClearResetsTimestamps** - Verifies clear() resets timestamp arrays
12. **testHasTimestamps** - Verifies version detection for rollback safety
13. **testV3StyleEntriesNoTimestamps** - Verifies v3 format entries work
14. **testEncoderDoesNotWriteTimestampsForV3** - Proves 32-byte difference between v3/v4 serialization
15. **testScanWithTimeRangeFiltering** - Integration: writes multi-block v4 HFile, scans with narrow time range, verifies block skipping
16. **testScanWithoutTimeRangeReadsAllBlocks** - Integration: verifies all blocks read without time range
17. **testScanV3HFileWithTimeRange** - Integration: verifies v3 files work with time range scans
18. **testScanWithNonMatchingTimeRange** - Integration: verifies non-overlapping time range behavior
19. **testNonRootTimestampRoundTrip** - Non-root index serialization/deserialization round-trip; verifies binary search, key extraction, and offset/size/timestamp reading with the 28-byte entry overhead
20. **testEntryOverheadWithTimestampsConstant** - Verifies SECONDARY_INDEX_ENTRY_OVERHEAD (12) and SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS (28)

## Performance Impact

### Benefits (v4 format)

**Time-bounded scans:**
- Skip irrelevant blocks without I/O
- Skip decompression overhead
- Reduce CPU usage for cell filtering

**Expected improvement:**
- Workload-dependent (depends on time range selectivity)

### Overhead (v4 format)

**Storage:**
- +16 bytes per block
- ~0.025% for 64KB blocks
- Negligible

**Write path:**
- Timestamp tracking
- Already tracking timestamps at file level
- Marginal

**Memory:**
- +16 bytes per block in block index cache
- Proportional to existing index memory
- Minimal impact

### Overhead (v3 format - default)

**No overhead:** Behaves identically to previous versions.

## Risks and Mitigations

### Risk 1: Filtering Bug Causes Data Loss

**Risk:** Incorrect filtering skips blocks that should be read.

**Mitigation:**
- Conservative filtering: when in doubt, read the block
- Extensive unit tests for edge cases (overlap detection, boundary conditions)
- TimeRange overlap logic reuses existing, well-tested code

### Risk 2: Version Mismatch Causes File Corruption

**Risk:** v3 readers attempt to read v4 files, or vice versa.

**Mitigation:**
- Version enforcement at construction (compile-time safety)
- Symmetric version checks in writer and reader
- `final` minorVersion fields (immutable)
- Tests verify exact byte-level differences between formats

### Risk 3: Rollback Complexity

**Risk:** Upgrading to v4 prevents rollback to v3 HBase.

**Mitigation:**
- **Default to v3 format** (conservative, rollback-safe)
- v4 is opt-in via explicit configuration
- Clear rollback procedure documented
- v4 code can write v3 files (configurable)

### Risk 4: DELETE Markers Missed by Block Skipping

**Risk:** A block containing a DELETE marker is skipped because its timestamps
don't overlap with the scan's time range. The DELETE never reaches the
`ScanDeleteTracker`, so a cell it should suppress is incorrectly returned.

**Why this is safe — built-in protection in `TimeRangeTracker`:**

`TimeRangeTracker.includeTimestamp(Cell)` (called by
`HFileBlock.Writer.trackTimestamp()` for every cell written to a data block)
has special handling for range-affecting delete types:

```java
public void includeTimestamp(final Cell cell) {
    includeTimestamp(cell.getTimestamp());
    if (PrivateCellUtil.isDeleteColumnOrFamily(cell)) {
        includeTimestamp(0);  // extends block minTs to 0
    }
}
```

This splits deletes into two categories:

| Delete type | Affects cells at other timestamps? | `includeTimestamp(0)` called? | Can block be skipped? |
|---|---|---|---|
| **DeleteColumn** (type 12) | Yes — all versions with ts ≤ marker ts | Yes | No — minTs=0 overlaps any range |
| **DeleteFamily** (type 14) | Yes — all columns/versions with ts ≤ marker ts | Yes | No — minTs=0 overlaps any range |
| **Delete / DeleteVersion** (type 8) | No — only the exact same timestamp | No | Yes — safe because target cell has same ts |
| **DeleteFamilyVersion** (type 10) | No — only the exact same timestamp | No | Yes — safe because target cell has same ts |

The two dangerous delete types (DeleteColumn, DeleteFamily) force
`blockMinTs = 0`, making the block's time range `[0, markerTs]`. This overlaps
with any scan time range, so the block is never skipped. The two safe delete
types (Delete, DeleteFamilyVersion) only suppress cells at their own timestamp,
so if the delete is outside the scan range, the cell it targets is also outside
the range.

### Risk 5: Memory Impact

**Risk:** Additional timestamp arrays increase heap pressure.

**Mitigation:**
- 16 bytes per block is minimal
- Proportional to existing block index (same number of entries)
- Block index is already cached, so no new cache pressure

## Deployment Recommendations

### Conservative Approach (Recommended)

**Phase 1: Upgrade to HBase 2.6.4+ (no config change)**
- All new files written in v3 format automatically
- Fully rollback-safe
- No performance change
- Validate stability for weeks/months

**Phase 2: Enable v4 after validation (explicit config)**
- Set `hfile.format.minor.version=4`
- Rolling restart
- New files use v4 format
- Old v3 files remain readable
- Gradually compact tables to v4

## Appendix: Version Constants

```java
// HFileReaderImpl.java
static final int MAX_MINOR_VERSION = 4;
static final int MINOR_VERSION_WITH_FAKED_KEY = 3;
static final int MINOR_VERSION_WITH_BLOCK_TIMERANGE = 4;

// HFile.java
public static final String FORMAT_MINOR_VERSION_KEY = "hfile.format.minor.version";
public static final int DEFAULT_MINOR_VERSION = 3;

// HFileBlockIndex.java
static final int SECONDARY_INDEX_ENTRY_OVERHEAD = 12;               // offset(8) + size(4)
static final int SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS = 28; // + minTs(8) + maxTs(8)
```

## Appendix: File Locations

**Core Implementation:**
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/HFileBlock.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/HFileWriterImpl.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/HFileBlockIndex.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/BlockIndexChunk.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/NoOpIndexBlockEncoder.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/HFile.java`
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/hfile/HFileReaderImpl.java`

**Tests:**
- `hbase-server/src/test/java/org/apache/hadoop/hbase/io/hfile/TestHFileBlockTimestamp.java`

