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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.Test;

public class CloneSnapshotFromClientAfterSplittingRegionTestBase
  extends CloneSnapshotFromClientTestBase {

  private void splitRegion() throws IOException {
    try (Table k = TEST_UTIL.getConnection().getTable(tableName);
      ResultScanner scanner = k.getScanner(new Scan())) {
      // Split on the second row to make sure that the snapshot contains reference files.
      // We also disable the compaction so that the reference files are not compacted away.
      scanner.next();
      admin.split(tableName, scanner.next().getRow());
    }
  }

  @Test
  public void testCloneSnapshotAfterSplittingRegion() throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

      // Split a region
      splitRegion();

      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName =
        TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);

      RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();

      // The region count of the cloned table should be the same as the one of the original table
      int openRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();
      int openRegionCountOfClonedTable =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.OPEN).size();
      assertEquals(openRegionCountOfOriginalTable, openRegionCountOfClonedTable);

      int splitRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT).size();
      List<RegionInfo> splitParents =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.SPLIT);
      int splitRegionCountOfClonedTable = splitParents.size();
      assertEquals(splitRegionCountOfOriginalTable, splitRegionCountOfClonedTable);

      // Make sure that the meta table was updated with the correct split information
      for (RegionInfo splitParent : splitParents) {
        Result result = MetaTableAccessor.getRegionResult(TEST_UTIL.getConnection(), splitParent);
        for (RegionInfo daughter : MetaTableAccessor.getDaughterRegions(result)) {
          assertNotNull(daughter);
        }
      }

      TEST_UTIL.deleteTable(clonedTableName);
    } finally {
      admin.catalogJanitorSwitch(true);
    }
  }

  /**
   * Regression test for the bug where HFiles referenced by HFileLink Reference files in a cloned
   * table are deleted from the archive by {@code HFileLinkCleaner} after the split-parent region
   * is GC'd.
   *
   * <p>Sequence that triggers the bug:
   * <ol>
   *   <li>Split a region — daughters get split reference files pointing at the parent's HFiles.</li>
   *   <li>Snapshot the table while reference files still exist (compaction disabled).</li>
   *   <li>Clone the snapshot — {@code RestoreSnapshotHelper.restoreReferenceFile()} creates
   *       HFileLink Reference files in the clone pointing at the split parent's HFiles, but does
   *       NOT create the back-references that {@code HFileLinkCleaner} relies on.</li>
   *   <li>Re-enable compaction and compact the daughter regions to resolve their references.</li>
   *   <li>CatalogJanitor GC archives the split-parent region's HFiles.</li>
   *   <li>{@code HFileLinkCleaner} deletes the archived HFiles because there are no
   *       back-references — breaking every read on the cloned table.</li>
   * </ol>
   *
   * <p>Without the fix, the final {@code verifyRowCount} call throws
   * {@code FileNotFoundException}.
   */
  @Test
  public void testCloneSnapshotAfterSplittingRegionAndGC() throws Exception {
    // Disable CatalogJanitor so GC doesn't race with snapshot/clone creation.
    admin.catalogJanitorSwitch(false);
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
    try {
      // Step 1: split a region. Compaction is globally disabled in setupConfiguration(), so the
      // daughter regions will retain split reference files pointing at the parent's HFiles.
      int numRegionsBefore = admin.getRegions(tableName).size();
      splitRegion();
      // Wait until the split completes and daughters are OPEN.
      TEST_UTIL.waitFor(30_000, () -> admin.getRegions(tableName).size() > numRegionsBefore);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

      // Step 2: snapshot while reference files still exist.
      admin.snapshot(snapshotName2, tableName);

      // Step 3: clone the snapshot.
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);
      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);

      // Step 4: re-enable compaction and compact the daughter regions to resolve references.
      setCompactionsEnabled(true);
      admin.majorCompact(tableName);
      // Wait until all daughter regions have no reference files.
      FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
      Path tableDir = CommonFSUtils.getTableDir(
        CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration()), tableName);
      TEST_UTIL.waitFor(60_000, () -> {
        try {
          for (int i = 0; i < TEST_UTIL.getHBaseCluster().getNumLiveRegionServers(); i++) {
            HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(i);
            rs.getCompactedHFilesDischarger().chore();
            for (HRegion region : rs.getRegions(tableName)) {
              RegionInfo ri = region.getRegionInfo();
              HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
                TEST_UTIL.getConfiguration(), fs, tableDir, ri, true);
              for (Path familyDir : FSUtils.getFamilyDirs(fs,
                new Path(tableDir, ri.getEncodedName()))) {
                org.apache.hadoop.hbase.regionserver.StoreContext storeContext =
                  org.apache.hadoop.hbase.regionserver.StoreContext.getBuilder()
                    .withColumnFamilyDescriptor(
                      org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
                        .of(familyDir.getName()))
                    .withRegionFileSystem(regionFs).withFamilyStoreDirectoryPath(familyDir)
                    .build();
                if (StoreFileTrackerFactory.create(TEST_UTIL.getConfiguration(), false,
                  storeContext).hasReferences()) {
                  return false;
                }
              }
            }
          }
          return true;
        } catch (IOException e) {
          return false;
        }
      });

      // Step 5: let CatalogJanitor GC the split-parent region (archives its HFiles).
      admin.catalogJanitorSwitch(true);
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogJanitor().choreForTesting();
      // Wait for parent region directory to be removed from the live table dir.
      RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
      List<RegionInfo> splitParents =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT);
      TEST_UTIL.waitFor(30_000, () -> {
        try {
          for (RegionInfo parent : splitParents) {
            if (fs.exists(new Path(tableDir, parent.getEncodedName()))) {
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          return false;
        }
      });

      // Step 6: expire the archive TTL and run HFileCleaner.
      Path archivePath = HFileArchiveUtil.getArchivePath(TEST_UTIL.getConfiguration());
      long expiredTime =
        EnvironmentEdgeManager.currentTime() - TimeToLiveHFileCleaner.DEFAULT_TTL * 1000;
      setFileTimesRecursively(fs, archivePath, expiredTime);
      TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().triggerCleanerNow().get();

      // Step 7: the clone must still be fully readable — its HFileLink References must still resolve.
      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);
    } finally {
      setCompactionsEnabled(true);
      admin.catalogJanitorSwitch(true);
      if (admin.tableExists(clonedTableName)) {
        TEST_UTIL.deleteTable(clonedTableName);
      }
    }
  }

  private void setCompactionsEnabled(boolean enabled) {
    for (int i = 0; i < TEST_UTIL.getHBaseCluster().getNumLiveRegionServers(); i++) {
      TEST_UTIL.getHBaseCluster().getRegionServer(i)
        .getCompactSplitThread().setCompactionsEnabled(enabled);
    }
  }

  private void setFileTimesRecursively(FileSystem fs, Path path, long time) throws IOException {
    fs.setTimes(path, time, -1);
    if (fs.isDirectory(path)) {
      for (FileStatus child : fs.listStatus(path)) {
        setFileTimesRecursively(fs, child.getPath(), time);
      }
    }
  }

  @Test
  public void testCloneSnapshotBeforeSplittingRegionAndDroppingTable()
    throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName =
        TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      // Split a region of the original table
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);
      splitRegion();

      // Drop the original table
      admin.disableTable(tableName);
      admin.deleteTable(tableName);

      // Disable and enable the cloned table. This should be successful
      admin.disableTable(clonedTableName);
      admin.enableTable(clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);
    } finally {
      admin.catalogJanitorSwitch(true);
    }
  }
}
