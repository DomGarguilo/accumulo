/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.TSERVER_WAL_SORT_CONCURRENT_POOL;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.manager.thrift.RecoveryStatus;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

public class LogSorter {

  private static final Logger log = LoggerFactory.getLogger(LogSorter.class);

  private final Map<String,LogProcessor> currentWork = Collections.synchronizedMap(new HashMap<>());

  class LogProcessor implements Processor {

    private FSDataInputStream input;
    private DataInputStream decryptingInput;
    private long bytesCopied = -1;
    private long sortStart = 0;
    private long sortStop = -1;

    @Override
    public Processor newProcessor() {
      return new LogProcessor();
    }

    @Override
    public void process(String child, byte[] data) {
      String work = new String(data, UTF_8);
      String[] parts = work.split("\\|");
      String src = parts[0];
      String dest = parts[1];
      String sortId = new Path(src).getName();
      log.debug("Sorting {} to {} using sortId {}", src, dest, sortId);

      synchronized (currentWork) {
        if (currentWork.containsKey(sortId)) {
          return;
        }
        currentWork.put(sortId, this);
      }

      synchronized (this) {
        sortStart = System.currentTimeMillis();
      }

      VolumeManager fs = context.getVolumeManager();

      String formerThreadName = Thread.currentThread().getName();
      try {
        sort(fs, sortId, new Path(src), dest);
      } catch (Exception t) {
        try {
          // parent dir may not exist
          fs.mkdirs(new Path(dest));
          fs.create(SortedLogState.getFailedMarkerPath(dest)).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + sortId, e);
        }
        log.error("Caught exception", t);
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (Exception e) {
          log.error("Error during cleanup sort/copy " + sortId, e);
        }
        synchronized (this) {
          sortStop = System.currentTimeMillis();
        }
        currentWork.remove(sortId);
      }
    }

    public void sort(VolumeManager fs, String name, Path srcPath, String destPath)
        throws IOException {
      int part = 0;

      // check for finished first since another thread may have already done the sort
      if (fs.exists(SortedLogState.getFinishedMarkerPath(destPath))) {
        log.debug("Sorting already finished at {}", destPath);
        return;
      }

      log.info("Copying {} to {}", srcPath, destPath);
      // the following call does not throw an exception if the file/dir does not exist
      fs.deleteRecursively(new Path(destPath));

      input = fs.open(srcPath);

      // Tell the DataNode that the write ahead log does not need to be cached in the OS page cache
      try {
        input.setDropBehind(Boolean.TRUE);
      } catch (UnsupportedOperationException e) {
        log.debug("setDropBehind reads not enabled for wal file: {}", input);
      } catch (IOException e) {
        log.debug("IOException setting drop behind for file: {}, msg: {}", input, e.getMessage());
      }

      try {
        decryptingInput = DfsLogger.getDecryptingStream(input, cryptoService);
      } catch (LogHeaderIncompleteException e) {
        log.warn("Could not read header from write-ahead log {}. Not sorting.", srcPath);
        // Creating a 'finished' marker will cause recovery to proceed normally and the
        // empty file will be correctly ignored downstream.
        fs.mkdirs(new Path(destPath));
        writeBuffer(destPath, Collections.emptyList(), part++);
        fs.create(SortedLogState.getFinishedMarkerPath(destPath)).close();
        return;
      }

      final long bufferSize = sortedLogConf.getAsBytes(Property.TSERV_WAL_SORT_BUFFER_SIZE);
      Thread.currentThread().setName("Sorting " + name + " for recovery");
      while (true) {
        final ArrayList<Pair<LogFileKey,LogFileValue>> buffer = new ArrayList<>();
        try {
          long start = input.getPos();
          while (input.getPos() - start < bufferSize) {
            LogFileKey key = new LogFileKey();
            LogFileValue value = new LogFileValue();
            key.readFields(decryptingInput);
            value.readFields(decryptingInput);
            buffer.add(new Pair<>(key, value));
          }
          writeBuffer(destPath, buffer, part++);
          buffer.clear();
        } catch (EOFException ex) {
          writeBuffer(destPath, buffer, part++);
          break;
        }
      }
      fs.create(new Path(destPath, "finished")).close();
      log.info("Finished log sort {} {} bytes {} parts in {}ms", name, getBytesCopied(), part,
          getSortTime());
    }

    synchronized void close() throws IOException {
      // If we receive an empty or malformed-header WAL, we won't
      // have input streams that need closing. Avoid the NPE.
      if (input != null) {
        bytesCopied = input.getPos();
        input.close();
        if (decryptingInput != null) {
          decryptingInput.close();
        }
        input = null;
      }
    }

    public synchronized long getSortTime() {
      if (sortStart > 0) {
        if (sortStop > 0) {
          return sortStop - sortStart;
        }
        return System.currentTimeMillis() - sortStart;
      }
      return 0;
    }

    synchronized long getBytesCopied() throws IOException {
      return input == null ? bytesCopied : input.getPos();
    }
  }

  private final AbstractServer server;
  private final ServerContext context;
  private final AccumuloConfiguration conf;
  private final double walBlockSize;
  private final CryptoService cryptoService;
  private final AccumuloConfiguration sortedLogConf;

  public LogSorter(AbstractServer server) {
    this.server = server;
    this.context = this.server.getContext();
    this.conf = this.context.getConfiguration();
    this.sortedLogConf = extractSortedLogConfig(this.conf);
    this.walBlockSize = DfsLogger.getWalBlockSize(this.conf);
    CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.RECOVERY);
    this.cryptoService =
        context.getCryptoFactory().getService(env, this.conf.getAllCryptoProperties());
  }

  /**
   * Get the properties set with {@link Property#TSERV_WAL_SORT_FILE_PREFIX} and translate them to
   * equivalent 'table.file' properties to be used when writing rfiles for sorted recovery.
   */
  private AccumuloConfiguration extractSortedLogConfig(AccumuloConfiguration conf) {
    final String tablePrefix = "table.file.";
    var props = conf.getAllPropertiesWithPrefixStripped(Property.TSERV_WAL_SORT_FILE_PREFIX);
    ConfigurationCopy copy = new ConfigurationCopy(conf);
    props.forEach((prop, val) -> {
      String tableProp = tablePrefix + prop;
      if (Property.isValidProperty(tableProp, val) && Property.isValidTablePropertyKey(tableProp)) {
        log.debug("Using property for writing sorted files: {}={}", tableProp, val);
        copy.set(tableProp, val);
      } else {
        throw new IllegalArgumentException("Invalid sort file property " + prop + "=" + val);
      }
    });
    return copy;
  }

  @VisibleForTesting
  void writeBuffer(String destPath, List<Pair<LogFileKey,LogFileValue>> buffer, int part)
      throws IOException {
    String filename = String.format("part-r-%05d.rf", part);
    Path path = new Path(destPath, filename);
    FileSystem fs = context.getVolumeManager().getFileSystemByPath(path);
    Path fullPath = fs.makeQualified(path);

    // convert the LogFileKeys to Keys, sort and collect the mutations
    Map<Key,List<Mutation>> keyListMap = new TreeMap<>();
    for (Pair<LogFileKey,LogFileValue> pair : buffer) {
      var logFileKey = pair.getFirst();
      var logFileValue = pair.getSecond();
      Key k = logFileKey.toKey();
      keyListMap.computeIfAbsent(k, (key) -> new ArrayList<>()).addAll(logFileValue.getMutations());
    }

    try (var writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(UnreferencedTabletFile.of(fs, fullPath), fs, fs.getConf(), cryptoService)
        .withTableConfiguration(sortedLogConf).build()) {
      writer.startDefaultLocalityGroup();
      for (var entry : keyListMap.entrySet()) {
        LogFileValue val = new LogFileValue();
        val.setMutations(entry.getValue());
        writer.append(entry.getKey(), val.toValue());
      }
    }
  }

  /**
   * Sort any logs that need sorting in the current thread.
   *
   * @return The time in millis when the next check can be done.
   */
  public long sortLogsIfNeeded() throws KeeperException, InterruptedException {
    DistributedWorkQueue dwq = new DistributedWorkQueue(Constants.ZRECOVERY, sortedLogConf, server);
    dwq.processExistingWork(new LogProcessor(), MoreExecutors.newDirectExecutorService(), 1, false);
    return System.currentTimeMillis() + dwq.getCheckInterval();
  }

  /**
   * Sort any logs that need sorting in a ThreadPool using
   * {@link Property#TSERV_WAL_SORT_MAX_CONCURRENT} threads. This method will start a background
   * thread to look for log sorting work in the future that will be processed by the
   * ThreadPoolExecutor
   */
  public void startWatchingForRecoveryLogs(int threadPoolSize)
      throws KeeperException, InterruptedException {
    ThreadPoolExecutor threadPool =
        ThreadPools.getServerThreadPools().getPoolBuilder(TSERVER_WAL_SORT_CONCURRENT_POOL)
            .numCoreThreads(threadPoolSize).enableThreadPoolMetrics().build();
    new DistributedWorkQueue(Constants.ZRECOVERY, sortedLogConf, server)
        .processExistingAndFuture(new LogProcessor(), threadPool);
  }

  public List<RecoveryStatus> getLogSorts() {
    List<RecoveryStatus> result = new ArrayList<>();
    synchronized (currentWork) {
      for (Entry<String,LogProcessor> entries : currentWork.entrySet()) {
        RecoveryStatus status = new RecoveryStatus();
        status.name = entries.getKey();
        try {
          double progress = entries.getValue().getBytesCopied() / walBlockSize;
          // to be sure progress does not exceed 100%
          status.progress = Math.min(progress, 99.9);
        } catch (IOException ex) {
          log.warn("Error getting bytes read");
        }
        status.runtime = (int) entries.getValue().getSortTime();
        result.add(status);
      }
      return result;
    }
  }
}
