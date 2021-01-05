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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The mob compaction thread used in {@link MasterRpcServices}
 */
@InterfaceAudience.Private
public class MasterMobCompactionThread {
  static final Logger LOG = LoggerFactory.getLogger(MasterMobCompactionThread.class);
  private final HMaster master;
  private final Configuration conf;
  private final ExecutorService mobCompactorPool;
  private final ExecutorService masterMobPool;

  public MasterMobCompactionThread(HMaster master) {
    this.master = master;
    this.conf = master.getConfiguration();
    final String n = Thread.currentThread().getName();
    // this pool is used to run the mob compaction
    this.masterMobPool = new ThreadPoolExecutor(1, 2, 60, TimeUnit.SECONDS,
      new SynchronousQueue<>(), new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(n + "-MasterMobCompaction-" + EnvironmentEdgeManager.currentTime()).build());
    ((ThreadPoolExecutor) this.masterMobPool).allowCoreThreadTimeOut(true);
    // this pool is used in the mob compaction to compact the mob files by partitions
    // in parallel
    this.mobCompactorPool = MobUtils.createMobCompactorThreadPool(master.getConfiguration());
  }

  /**
   * Requests mob compaction
   * @param conf     The Configuration
   * @param fs       The file system
   * @param td       The descriptor of the table to compact
   * @param columns  The column descriptors
   * @param allFiles Whether add all mob files into the compaction.
   */
  public void requestMobCompaction(Configuration conf, FileSystem fs, TableDescriptor td,
    List<ColumnFamilyDescriptor> columns, boolean allFiles) throws IOException {
    master.reportMobCompactionStart(td.getTableName());
    try {
      masterMobPool.execute(new CompactionRunner(fs, td, columns, allFiles, mobCompactorPool));
    } catch (RejectedExecutionException e) {
      // in case the request is rejected by the pool
      try {
        master.reportMobCompactionEnd(td.getTableName());
      } catch (IOException e1) {
        LOG.error("Failed to mark end of mob compaction", e1);
      }
      throw e;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The mob compaction is requested for the columns " + columns + " of the table "
        + td.getTableName().getNameAsString());
    }
  }

  private class CompactionRunner implements Runnable {
    private FileSystem fs;
    private TableDescriptor tableDescriptor;
    private List<ColumnFamilyDescriptor> hcds;
    private boolean allFiles;
    private ExecutorService pool;

    public CompactionRunner(FileSystem fs, TableDescriptor td, List<ColumnFamilyDescriptor> hcds,
      boolean allFiles, ExecutorService pool) {
      super();
      this.fs = fs;
      this.tableDescriptor = td;
      this.hcds = hcds;
      this.allFiles = allFiles;
      this.pool = pool;
    }

    @Override
    public void run() {
      // These locks are on dummy table names, and only used for compaction/mob file cleaning.
      final LockManager.MasterLock lock = master.getLockManager().createMasterLock(
        MobUtils.getTableLockName(tableDescriptor.getTableName()), LockType.EXCLUSIVE,
        this.getClass().getName() + ": mob compaction");
      try {
        for (ColumnFamilyDescriptor hcd : hcds) {
          MobUtils.doMobCompaction(conf, fs, tableDescriptor, hcd, pool, allFiles, lock);
        }
      } catch (IOException e) {
        LOG.error("Failed to perform the mob compaction", e);
      } finally {
        try {
          master.reportMobCompactionEnd(tableDescriptor.getTableName());
        } catch (IOException e) {
          LOG.error("Failed to mark end of mob compaction", e);
        }
      }
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  private void interruptIfNecessary() {
    mobCompactorPool.shutdown();
    masterMobPool.shutdown();
  }

  /**
   * Wait for all the threads finish.
   */
  private void join() {
    waitFor(mobCompactorPool, "Mob Compaction Thread");
    waitFor(masterMobPool, "Region Server Mob Compaction Thread");
  }

  /**
   * Closes the MasterMobCompactionThread.
   */
  public void close() {
    interruptIfNecessary();
    join();
  }

  /**
   * Wait for thread finish.
   * @param t    the thread to wait
   * @param name the thread name.
   */
  private void waitFor(ExecutorService t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Waiting for " + name + " to finish...");
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
      }
    }
  }
}
