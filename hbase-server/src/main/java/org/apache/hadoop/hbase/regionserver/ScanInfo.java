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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Immutable information for scans over a store.
 */
// Has to be public for PartitionedMobCompactor to access; ditto on tests making use of a few of
// the accessors below. Shutdown access. TODO
@InterfaceAudience.Private
public class ScanInfo {
  private byte[] family;
  private int minVersions;
  private int maxVersions;
  private long ttl;
  private KeepDeletedCells keepDeletedCells;
  private long timeToPurgeDeletes;
  private CellComparator comparator;
  private long tableMaxRowSize;
  private boolean usePread;
  private long cellsPerTimeoutCheck;
  private boolean parallelSeekEnabled;
  private final long preadMaxBytes;
  private final boolean newVersionBehavior;
  private final boolean nanosecondTimestamps;

  public static final long FIXED_OVERHEAD =
    ClassSize.align(ClassSize.OBJECT + (2 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_INT)
      + (4 * Bytes.SIZEOF_LONG) + (4 * Bytes.SIZEOF_BOOLEAN));

  /**
   * @param table      {@link TableDescriptor} describing the table
   * @param family     {@link ColumnFamilyDescriptor} describing the column family
   * @param comparator The store's comparator
   */
  public ScanInfo(Configuration conf, TableDescriptor table, ColumnFamilyDescriptor family,
    CellComparator comparator) {
    this(conf, family.getName(), family.getMinVersions(), family.getMaxVersions(),
      determineTTL(family, table.isNanosecondTimestamps()), family.getKeepDeletedCells(),
      family.getBlocksize(), determineTimeToPurgeDeletes(conf, table.isNanosecondTimestamps()),
      comparator, family.isNewVersionBehavior(), table.isNanosecondTimestamps());
  }

  private static long getCellsPerTimeoutCheck(Configuration conf) {
    long perHeartbeat = conf.getLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
      StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);
    return perHeartbeat > 0
      ? perHeartbeat
      : StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;
  }

  /**
   * @param family               Name of this store's column family
   * @param minVersions          Store's MIN_VERSIONS setting
   * @param maxVersions          Store's VERSIONS setting
   * @param ttl                  Store's TTL (in ms)
   * @param blockSize            Store's block size
   * @param timeToPurgeDeletes   duration in ms after which a delete marker can be purged during a
   *                             major compaction.
   * @param keepDeletedCells     Store's keepDeletedCells setting
   * @param comparator           The store's comparator
   * @param newVersionBehavior   whether compare cells by MVCC when scanning
   * @param nanosecondTimestamps whether treat timestamps as nanoseconds
   */
  public ScanInfo(Configuration conf, byte[] family, int minVersions, int maxVersions, long ttl,
    KeepDeletedCells keepDeletedCells, long blockSize, long timeToPurgeDeletes,
    CellComparator comparator, boolean newVersionBehavior, boolean nanosecondTimestamps) {
    this(family, minVersions, maxVersions, ttl, keepDeletedCells, timeToPurgeDeletes, comparator,
      conf.getLong(HConstants.TABLE_MAX_ROWSIZE_KEY, HConstants.TABLE_MAX_ROWSIZE_DEFAULT),
      conf.getBoolean("hbase.storescanner.use.pread", false), getCellsPerTimeoutCheck(conf),
      conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false),
      conf.getLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 4 * blockSize), newVersionBehavior,
      nanosecondTimestamps);
  }

  private ScanInfo(byte[] family, int minVersions, int maxVersions, long ttl,
    KeepDeletedCells keepDeletedCells, long timeToPurgeDeletes, CellComparator comparator,
    long tableMaxRowSize, boolean usePread, long cellsPerTimeoutCheck, boolean parallelSeekEnabled,
    long preadMaxBytes, boolean newVersionBehavior, boolean nanosecondTimestamps) {
    this.family = family;
    this.minVersions = minVersions;
    this.maxVersions = maxVersions;
    this.ttl = ttl;
    this.keepDeletedCells = keepDeletedCells;
    this.timeToPurgeDeletes = timeToPurgeDeletes;
    this.comparator = comparator;
    this.tableMaxRowSize = tableMaxRowSize;
    this.usePread = usePread;
    this.cellsPerTimeoutCheck = cellsPerTimeoutCheck;
    this.parallelSeekEnabled = parallelSeekEnabled;
    this.preadMaxBytes = preadMaxBytes;
    this.newVersionBehavior = newVersionBehavior;
    this.nanosecondTimestamps = nanosecondTimestamps;
  }

  /**
   * @param conf                   is global configuration
   * @param isNanosecondTimestamps whether timestamps are treated as nanoseconds
   * @return TTL for deleted cells. Default in milliseconds and in nanoseconds if
   *         NANOSECOND_TIMESTAMPS table attribute is provided.
   */
  private static long determineTimeToPurgeDeletes(final Configuration conf,
    final boolean isNanosecondTimestamps) {
    long ttpd = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    if (isNanosecondTimestamps) {
      ttpd *= 1000000;
    }
    return ttpd;
  }

  /**
   * @param family                 is store's family
   * @param isNanosecondTimestamps whether timestamps are treated as nanoseconds
   * @return TTL of the specified column family. Default in milliseconds and in nanoseconds if
   *         NANOSECOND_TIMESTAMPS table attribute is provided.
   */
  private static long determineTTL(final ColumnFamilyDescriptor family,
    final boolean isNanosecondTimestamps) {
    // ColumnFamilyDescriptor.getTimeToLive() returns ttl in seconds.
    long ttl = family.getTimeToLive();
    if (ttl == HConstants.FOREVER) {
      // Default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else if (isNanosecondTimestamps) {
      // Second -> ns adjust for user data
      ttl *= 1000000000;
    } else {
      // Second -> ms adjust for user data
      ttl *= 1000;
    }
    return ttl;
  }

  long getTableMaxRowSize() {
    return this.tableMaxRowSize;
  }

  boolean isUsePread() {
    return this.usePread;
  }

  long getCellsPerTimeoutCheck() {
    return this.cellsPerTimeoutCheck;
  }

  boolean isParallelSeekEnabled() {
    return this.parallelSeekEnabled;
  }

  public byte[] getFamily() {
    return family;
  }

  public int getMinVersions() {
    return minVersions;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public long getTtl() {
    return ttl;
  }

  public KeepDeletedCells getKeepDeletedCells() {
    return keepDeletedCells;
  }

  public long getTimeToPurgeDeletes() {
    return timeToPurgeDeletes;
  }

  public CellComparator getComparator() {
    return comparator;
  }

  long getPreadMaxBytes() {
    return preadMaxBytes;
  }

  public boolean isNewVersionBehavior() {
    return newVersionBehavior;
  }

  /**
   * Whether ScanInfo is for table that assumes nanosecond timestamps.
   * @return true if nanosecond timestamps.
   */
  public boolean isNanosecondTimestamps() {
    return nanosecondTimestamps;
  }

  /**
   * Used for CP users for customizing max versions, ttl and keepDeletedCells.
   */
  ScanInfo customize(int maxVersions, long ttl, KeepDeletedCells keepDeletedCells) {
    return customize(maxVersions, ttl, keepDeletedCells, minVersions, timeToPurgeDeletes);
  }

  /**
   * Used by CP users for customizing max versions, ttl, keepDeletedCells, min versions, and time to
   * purge deletes.
   */
  ScanInfo customize(int maxVersions, long ttl, KeepDeletedCells keepDeletedCells, int minVersions,
    long timeToPurgeDeletes) {
    return new ScanInfo(family, minVersions, maxVersions, ttl, keepDeletedCells, timeToPurgeDeletes,
      comparator, tableMaxRowSize, usePread, cellsPerTimeoutCheck, parallelSeekEnabled,
      preadMaxBytes, newVersionBehavior, nanosecondTimestamps);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("family", Bytes.toStringBinary(family))
      .append("minVersions", minVersions).append("maxVersions", maxVersions).append("ttl", ttl)
      .append("keepDeletedCells", keepDeletedCells).append("timeToPurgeDeletes", timeToPurgeDeletes)
      .append("tableMaxRowSize", tableMaxRowSize).append("usePread", usePread)
      .append("cellsPerTimeoutCheck", cellsPerTimeoutCheck)
      .append("parallelSeekEnabled", parallelSeekEnabled).append("preadMaxBytes", preadMaxBytes)
      .append("newVersionBehavior", newVersionBehavior).toString();
  }
}
