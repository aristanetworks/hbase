/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MID_KEY_METADATA_SIZE;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Does not perform any kind of encoding/decoding.
 */
@InterfaceAudience.Private
public class NoOpIndexBlockEncoder implements HFileIndexBlockEncoder {

  public static final NoOpIndexBlockEncoder INSTANCE = new NoOpIndexBlockEncoder();

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpIndexBlockEncoder() {
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException {
    if (rootIndexBlock) {
      writeRoot(blockIndexChunk, out);
    } else {
      writeNonRoot(blockIndexChunk, out);
    }
  }

  /**
   * Writes the block index chunk in the non-root index block format. This format contains the
   * number of entries, an index of integer offsets for quick binary search on variable-length
   * records, and tuples of block offset, on-disk block size, optional timestamps, and the first
   * key for each entry. For HFile v4+, min/max timestamps are written between the on-disk size
   * and the key, producing a fixed entry overhead of 28 bytes instead of 12.
   */
  private void writeNonRoot(BlockIndexChunk blockIndexChunk, DataOutput out) throws IOException {
    // The number of entries in the block.
    out.writeInt(blockIndexChunk.getNumEntries());

    if (
      blockIndexChunk.getSecondaryIndexOffsetMarks().size() != blockIndexChunk.getBlockKeys().size()
    ) {
      throw new IOException("Corrupted block index chunk writer: "
        + blockIndexChunk.getBlockKeys().size() + " entries but "
        + blockIndexChunk.getSecondaryIndexOffsetMarks().size() + " secondary index items");
    }

    // For each entry, write a "secondary index" of relative offsets to the
    // entries from the end of the secondary index. This works, because at
    // read time we read the number of entries and know where the secondary
    // index ends.
    for (int currentSecondaryIndex : blockIndexChunk.getSecondaryIndexOffsetMarks())
      out.writeInt(currentSecondaryIndex);

    // We include one other element in the secondary index to calculate the
    // size of each entry more easily by subtracting secondary index elements.
    out.writeInt(blockIndexChunk.getCurTotalNonRootEntrySize());

    boolean hasTimestamps = blockIndexChunk.hasTimestamps();
    for (int i = 0; i < blockIndexChunk.getNumEntries(); ++i) {
      out.writeLong(blockIndexChunk.getBlockOffset(i));
      out.writeInt(blockIndexChunk.getOnDiskDataSize(i));
      if (hasTimestamps) {
        out.writeLong(blockIndexChunk.getBlockMinTimestamp(i));
        out.writeLong(blockIndexChunk.getBlockMaxTimestamp(i));
      }
      out.write(blockIndexChunk.getBlockKey(i));
    }
  }

  /**
   * Writes this chunk into the given output stream in the root block index format. This format is
   * similar to the {@link HFile} version 1 block index format, except that we store on-disk size of
   * the block instead of its uncompressed size.
   * For HFile v4+, min/max timestamps are written between the on-disk size and the key, matching
   * the non-root index block layout.
   * @param out the data output stream to write the block index to. Typically a stream writing into
   *            an {@link HFile} block.
   */
  private void writeRoot(BlockIndexChunk blockIndexChunk, DataOutput out) throws IOException {
    boolean hasTimestamps = blockIndexChunk.hasTimestamps();
    for (int i = 0; i < blockIndexChunk.getNumEntries(); ++i) {
      out.writeLong(blockIndexChunk.getBlockOffset(i));
      out.writeInt(blockIndexChunk.getOnDiskDataSize(i));
      if (hasTimestamps) {
        out.writeLong(blockIndexChunk.getBlockMinTimestamp(i));
        out.writeLong(blockIndexChunk.getBlockMaxTimestamp(i));
      }
      Bytes.writeByteArray(out, blockIndexChunk.getBlockKey(i));
    }
  }

  @Override
  public IndexBlockEncoding getIndexBlockEncoding() {
    return IndexBlockEncoding.NONE;
  }

  @Override
  public EncodedSeeker createSeeker() {
    return new NoOpEncodedSeeker();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public static class NoOpEncodedSeeker implements EncodedSeeker {

    protected long[] blockOffsets;
    protected int[] blockDataSizes;
    protected int rootCount = 0;

    // Block timestamp metadata (HFile v4+)
    protected long[] blockMinTimestamps;
    protected long[] blockMaxTimestamps;

    // Mid-key metadata.
    protected long midLeafBlockOffset = -1;
    protected int midLeafBlockOnDiskSize = -1;
    protected int midKeyEntry = -1;

    private Cell[] blockKeys;
    private CellComparator comparator;
    protected int searchTreeLevel;

    // Minor version of the HFile being read
    private int minorVersion = 0;

    /** Pre-computed mid-key */
    private AtomicReference<Cell> midKey = new AtomicReference<>();

    @Override
    public long heapSize() {
      long heapSize = ClassSize.align(ClassSize.OBJECT);

      // Mid-key metadata.
      heapSize += MID_KEY_METADATA_SIZE;

      if (blockOffsets != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockOffsets.length * Bytes.SIZEOF_LONG);
      }

      if (blockDataSizes != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockDataSizes.length * Bytes.SIZEOF_INT);
      }

      // Block timestamp arrays (HFile v4+)
      if (blockMinTimestamps != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockMinTimestamps.length * Bytes.SIZEOF_LONG);
      }
      if (blockMaxTimestamps != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockMaxTimestamps.length * Bytes.SIZEOF_LONG);
      }

      if (blockKeys != null) {
        heapSize += ClassSize.REFERENCE;
        // Adding array + references overhead
        heapSize += ClassSize.align(ClassSize.ARRAY + blockKeys.length * ClassSize.REFERENCE);

        // Adding blockKeys
        for (Cell key : blockKeys) {
          heapSize += ClassSize.align(key.heapSize());
        }
      }
      // Add comparator and the midkey atomicreference
      heapSize += 2 * ClassSize.REFERENCE;
      // Add rootCount, searchTreeLevel, and minorVersion
      heapSize += 3 * Bytes.SIZEOF_INT;

      return ClassSize.align(heapSize);
    }

    @Override
    public boolean isEmpty() {
      return blockKeys.length == 0;
    }

    @Override
    public Cell getRootBlockKey(int i) {
      return blockKeys[i];
    }

    @Override
    public int getRootBlockCount() {
      return rootCount;
    }

    @Override
    public void initRootIndex(HFileBlock blk, int numEntries, CellComparator comparator,
      int treeLevel, int minorVersion) throws IOException {
      this.comparator = comparator;
      this.searchTreeLevel = treeLevel;
      this.minorVersion = minorVersion;
      init(blk, numEntries);
    }

    private void init(HFileBlock blk, int numEntries) throws IOException {
      DataInputStream in = readRootIndex(blk, numEntries);
      // HFileBlock.getByteStream() returns a byte stream for reading the data(excluding checksum)
      // of root index block, so after reading the root index there is no need to subtract the
      // checksum bytes.
      if (in.available() < MID_KEY_METADATA_SIZE) {
        // No mid-key metadata available.
        return;
      }
      midLeafBlockOffset = in.readLong();
      midLeafBlockOnDiskSize = in.readInt();
      midKeyEntry = in.readInt();
    }

    private DataInputStream readRootIndex(HFileBlock blk, final int numEntries) throws IOException {
      DataInputStream in = blk.getByteStream();
      readRootIndex(in, numEntries);
      return in;
    }

    private void readRootIndex(DataInput in, final int numEntries) throws IOException {
      blockOffsets = new long[numEntries];
      initialize(numEntries);
      blockDataSizes = new int[numEntries];

      boolean hasTimestamps =
        minorVersion >= HFileReaderImpl.MINOR_VERSION_WITH_BLOCK_TIMERANGE;
      if (hasTimestamps) {
        blockMinTimestamps = new long[numEntries];
        blockMaxTimestamps = new long[numEntries];
      }

      if (numEntries > 0) {
        for (int i = 0; i < numEntries; ++i) {
          long offset = in.readLong();
          int dataSize = in.readInt();

          long minTs = 0, maxTs = 0;
          if (hasTimestamps) {
            minTs = in.readLong();
            maxTs = in.readLong();
          }

          byte[] key = Bytes.readByteArray(in);

          if (hasTimestamps) {
            add(key, offset, dataSize, minTs, maxTs);
          } else {
            add(key, offset, dataSize);
          }
        }
      }
    }

    private void initialize(int numEntries) {
      blockKeys = new Cell[numEntries];
    }

    private void add(final byte[] key, final long offset, final int dataSize) {
      add(key, offset, dataSize,
        org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MIN_TIMESTAMP,
        org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MAX_TIMESTAMP);
    }

    private void add(final byte[] key, final long offset, final int dataSize, final long minTs,
      final long maxTs) {
      blockOffsets[rootCount] = offset;
      // Create the blockKeys as Cells once when the reader is opened
      blockKeys[rootCount] = new KeyValue.KeyOnlyKeyValue(key, 0, key.length);
      blockDataSizes[rootCount] = dataSize;

      // Store timestamps if arrays are allocated (HFile v4+)
      if (blockMinTimestamps != null && blockMaxTimestamps != null) {
        blockMinTimestamps[rootCount] = minTs;
        blockMaxTimestamps[rootCount] = maxTs;
      }

      rootCount++;
    }

    @Override
    public Cell midkey(HFile.CachingBlockReader cachingBlockReader) throws IOException {
      if (rootCount == 0) throw new IOException("HFile empty");

      Cell targetMidKey = this.midKey.get();
      if (targetMidKey != null) {
        return targetMidKey;
      }

      if (midLeafBlockOffset >= 0) {
        if (cachingBlockReader == null) {
          throw new IOException(
            "Have to read the middle leaf block but " + "no block reader available");
        }

        // Caching, using pread, assuming this is not a compaction.
        HFileBlock midLeafBlock = cachingBlockReader.readBlock(midLeafBlockOffset,
          midLeafBlockOnDiskSize, true, true, false, true, BlockType.LEAF_INDEX, null);
        try {
          int entryOverhead =
            minorVersion >= HFileReaderImpl.MINOR_VERSION_WITH_BLOCK_TIMERANGE
              ? HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS
              : HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
          byte[] bytes = HFileBlockIndex.BlockIndexReader
            .getNonRootIndexedKey(midLeafBlock.getBufferWithoutHeader(), midKeyEntry,
              entryOverhead);
          assert bytes != null;
          targetMidKey = new KeyValue.KeyOnlyKeyValue(bytes, 0, bytes.length);
        } finally {
          midLeafBlock.release();
        }
      } else {
        // The middle of the root-level index.
        targetMidKey = blockKeys[rootCount / 2];
      }

      this.midKey.set(targetMidKey);
      return targetMidKey;
    }

    @Override
    public BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
      boolean cacheBlocks, boolean pread, boolean isCompaction,
      DataBlockEncoding expectedDataBlockEncoding, HFile.CachingBlockReader cachingBlockReader)
      throws IOException {
      int rootLevelIndex = rootBlockContainingKey(key);
      if (rootLevelIndex < 0 || rootLevelIndex >= blockOffsets.length) {
        return null;
      }

      // the next indexed key
      Cell nextIndexedKey = null;

      // Read the next-level (intermediate or leaf) index block.
      long currentOffset = blockOffsets[rootLevelIndex];
      int currentOnDiskSize = blockDataSizes[rootLevelIndex];

      if (rootLevelIndex < blockKeys.length - 1) {
        nextIndexedKey = blockKeys[rootLevelIndex + 1];
      } else {
        nextIndexedKey = KeyValueScanner.NO_NEXT_INDEXED_KEY;
      }

      int lookupLevel = 1; // How many levels deep we are in our lookup.
      int index = -1;

      HFileBlock block = null;
      KeyValue.KeyOnlyKeyValue tmpNextIndexKV = new KeyValue.KeyOnlyKeyValue();
      while (true) {
        try {
          // Must initialize it with null here, because if don't and once an exception happen in
          // readBlock, then we'll release the previous assigned block twice in the finally block.
          // (See HBASE-22422)
          block = null;
          if (currentBlock != null && currentBlock.getOffset() == currentOffset) {
            // Avoid reading the same block again, even with caching turned off.
            // This is crucial for compaction-type workload which might have
            // caching turned off. This is like a one-block cache inside the
            // scanner.
            block = currentBlock;
          } else {
            // Call HFile's caching block reader API. We always cache index
            // blocks, otherwise we might get terrible performance.
            boolean shouldCache = cacheBlocks || (lookupLevel < searchTreeLevel);
            BlockType expectedBlockType;
            if (lookupLevel < searchTreeLevel - 1) {
              expectedBlockType = BlockType.INTERMEDIATE_INDEX;
            } else if (lookupLevel == searchTreeLevel - 1) {
              expectedBlockType = BlockType.LEAF_INDEX;
            } else {
              // this also accounts for ENCODED_DATA
              expectedBlockType = BlockType.DATA;
            }
            block = cachingBlockReader.readBlock(currentOffset, currentOnDiskSize, shouldCache,
              pread, isCompaction, true, expectedBlockType, expectedDataBlockEncoding);
          }

          if (block == null) {
            throw new IOException("Failed to read block at offset " + currentOffset
              + ", onDiskSize=" + currentOnDiskSize);
          }

          // Found a data block, break the loop and check our level in the tree.
          if (block.getBlockType().isData()) {
            break;
          }

          // Not a data block. This must be a leaf-level or intermediate-level
          // index block. We don't allow going deeper than searchTreeLevel.
          if (++lookupLevel > searchTreeLevel) {
            throw new IOException("Search Tree Level overflow: lookupLevel=" + lookupLevel
              + ", searchTreeLevel=" + searchTreeLevel);
          }

          // Locate the entry corresponding to the given key in the non-root
          // (leaf or intermediate-level) index block.
          ByteBuff buffer = block.getBufferWithoutHeader();
          int entryOverhead =
            minorVersion >= HFileReaderImpl.MINOR_VERSION_WITH_BLOCK_TIMERANGE
              ? HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD_WITH_TIMESTAMPS
              : HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
          index = HFileBlockIndex.BlockIndexReader.locateNonRootIndexEntry(buffer, key, comparator,
            entryOverhead);
          if (index == -1) {
            // This has to be changed
            // For now change this to key value
            throw new IOException("The key " + CellUtil.getCellKeyAsString(key) + " is before the"
              + " first key of the non-root index block " + block);
          }

          currentOffset = buffer.getLong();
          currentOnDiskSize = buffer.getInt();

          // Skip past timestamp fields for v4+ non-root index blocks
          if (minorVersion >= HFileReaderImpl.MINOR_VERSION_WITH_BLOCK_TIMERANGE) {
            buffer.getLong(); // skip minTimestamp
            buffer.getLong(); // skip maxTimestamp
          }

          // Only update next indexed key if there is a next indexed key in the current level
          byte[] nonRootIndexedKey =
            HFileBlockIndex.BlockIndexReader.getNonRootIndexedKey(buffer, index + 1,
              entryOverhead);
          if (nonRootIndexedKey != null) {
            tmpNextIndexKV.setKey(nonRootIndexedKey, 0, nonRootIndexedKey.length);
            nextIndexedKey = tmpNextIndexKV;
          }
        } finally {
          if (block != null && !block.getBlockType().isData()) {
            // Release the block immediately if it is not the data block
            block.release();
          }
        }
      }

      if (lookupLevel != searchTreeLevel) {
        assert block.getBlockType().isData();
        // Though we have retrieved a data block we have found an issue
        // in the retrieved data block. Hence returned the block so that
        // the ref count can be decremented
        if (block != null) {
          block.release();
        }
        throw new IOException("Reached a data block at level " + lookupLevel
          + " but the number of levels is " + searchTreeLevel);
      }

      // set the next indexed key for the current block.
      return new BlockWithScanInfo(block, nextIndexedKey);
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      // Here the comparator should not be null as this happens for the root-level block
      int pos = Bytes.binarySearch(blockKeys, key, comparator);
      // pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
      // binarySearch's javadoc.

      if (pos >= 0) {
        // This means this is an exact match with an element of blockKeys.
        assert pos < blockKeys.length;
        return pos;
      }

      // Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
      // and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
      // blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
      // key < blockKeys[0], meaning the file does not contain the given key.

      int i = -pos - 1;
      assert 0 <= i && i <= blockKeys.length;
      return i - 1;
    }

    /**
     * Get the minimum timestamp for a block at the given index.
     * @param blockIndex the block index
     * @return the minimum timestamp, or INITIAL_MIN_TIMESTAMP if not available
     */
    public long getBlockMinTimestamp(int blockIndex) {
      if (blockMinTimestamps != null && blockIndex >= 0 && blockIndex < blockMinTimestamps.length) {
        return blockMinTimestamps[blockIndex];
      }
      return org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MIN_TIMESTAMP;
    }

    /**
     * Get the maximum timestamp for a block at the given index.
     * @param blockIndex the block index
     * @return the maximum timestamp, or INITIAL_MAX_TIMESTAMP if not available
     */
    public long getBlockMaxTimestamp(int blockIndex) {
      if (blockMaxTimestamps != null && blockIndex >= 0 && blockIndex < blockMaxTimestamps.length) {
        return blockMaxTimestamps[blockIndex];
      }
      return org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MAX_TIMESTAMP;
    }

    /**
     * Find the block index for a given block offset.
     * @param blockOffset the block offset to find
     * @return the block index, or -1 if not found
     */
    public int getBlockIndexByOffset(long blockOffset) {
      if (blockOffsets == null) {
        return -1;
      }
      for (int i = 0; i < rootCount; i++) {
        if (blockOffsets[i] == blockOffset) {
          return i;
        }
      }
      return -1;
    }

    /**
     * Check if a block at the given offset should be read based on time range filtering.
     * @param blockOffset    the block offset
     * @param scanTimeRange  the scan's time range filter (null means no filtering)
     * @return true if the block should be read, false if it can be skipped
     */
    public boolean shouldReadBlockAtOffset(long blockOffset,
      org.apache.hadoop.hbase.io.TimeRange scanTimeRange) {
      // If no time range filter, read all blocks
      if (scanTimeRange == null) {
        return true;
      }

      // Find the block index for this offset
      int blockIndex = getBlockIndexByOffset(blockOffset);
      if (blockIndex < 0) {
        // Block not found in index - read it to be safe
        return true;
      }

      return shouldReadBlock(blockIndex, scanTimeRange);
    }

    /**
     * Check if a block should be read based on time range filtering.
     * @param blockIndex     the block index
     * @param scanTimeRange  the scan's time range filter
     * @return true if the block should be read, false if it can be skipped
     */
    public boolean shouldReadBlock(int blockIndex,
      org.apache.hadoop.hbase.io.TimeRange scanTimeRange) {
      // If no timestamp metadata available, must read the block (backward compatibility)
      if (blockMinTimestamps == null || blockMaxTimestamps == null) {
        return true;
      }

      // If block index out of range, should not read
      if (blockIndex < 0 || blockIndex >= blockMinTimestamps.length) {
        return false;
      }

      long blockMin = blockMinTimestamps[blockIndex];
      long blockMax = blockMaxTimestamps[blockIndex];

      // If timestamps are initial values (no data), read the block
      if (
        blockMin == org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MIN_TIMESTAMP
          || blockMax == org.apache.hadoop.hbase.regionserver.TimeRangeTracker.INITIAL_MAX_TIMESTAMP
      ) {
        return true;
      }

      // Check if block's time range overlaps with scan's time range
      org.apache.hadoop.hbase.io.TimeRange blockRange =
        org.apache.hadoop.hbase.io.TimeRange.between(blockMin, blockMax + 1);
      return scanTimeRange.includesTimeRange(blockRange);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size=" + rootCount).append("\n");
      for (int i = 0; i < rootCount; i++) {
        sb.append("key=").append((blockKeys[i])).append("\n  offset=").append(blockOffsets[i])
          .append(", dataSize=" + blockDataSizes[i]).append("\n");
      }
      return sb.toString();
    }
  }
}
