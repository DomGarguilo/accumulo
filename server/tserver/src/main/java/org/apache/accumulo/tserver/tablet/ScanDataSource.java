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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iteratorsImpl.system.StatsIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.conf.TableConfiguration.ParsedIteratorConfig;
import org.apache.accumulo.server.fs.FileManager.ScanFileManager;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironmentImpl;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanDataSource implements DataSource {

  private static final Logger log = LoggerFactory.getLogger(ScanDataSource.class);
  // data source state
  private final TabletBase tablet;
  private ScanFileManager fileManager;
  private static final AtomicLong nextSourceId = new AtomicLong(0);
  private SortedKeyValueIterator<Key,Value> iter;
  private long expectedDeletionCount;
  private List<MemoryIterator> memIters = null;
  private long fileReservationId;
  private final AtomicBoolean interruptFlag;
  private StatsIterator statsIterator;

  private final ScanParameters scanParams;
  private final boolean loadIters;
  private final byte[] defaultLabels;
  private final long scanDataSourceId;

  ScanDataSource(TabletBase tablet, ScanParameters scanParams, boolean loadIters,
      AtomicBoolean interruptFlag) {
    this.tablet = tablet;
    this.expectedDeletionCount = tablet.getDataSourceDeletions();
    this.scanParams = scanParams;
    this.interruptFlag = interruptFlag;
    this.loadIters = loadIters;
    this.defaultLabels = tablet.getDefaultSecurityLabels();
    this.scanDataSourceId = nextSourceId.incrementAndGet();
    log.trace("new scan data source, scanId {}, tablet: {}, params: {}, loadIterators: {}",
        this.scanDataSourceId, this.tablet, this.scanParams, this.loadIters);
  }

  @Override
  public DataSource getNewDataSource() {
    if (!isCurrent()) {
      Throwable thrownException = null;
      try {
        returnIterators();
      } catch (Exception e) {
        thrownException = e;
        throw e;
      } finally {
        try {
          if (fileManager != null) {
            fileManager.releaseOpenFiles(false);
          }
        } catch (Exception e) {
          if (thrownException != null) {
            e.addSuppressed(thrownException);
            throw e;
          }
        } finally {
          expectedDeletionCount = tablet.getDataSourceDeletions();
          iter = null;
        }
      }
    }
    return this;
  }

  @Override
  public boolean isCurrent() {
    return expectedDeletionCount == tablet.getDataSourceDeletions();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
    if (iter == null) {
      try {
        iter = createIterator();
      } catch (ReflectiveOperationException e) {
        throw new IOException("Error creating iterator", e);
      }
    }
    return iter;
  }

  private SortedKeyValueIterator<Key,Value> createIterator()
      throws IOException, ReflectiveOperationException {

    Map<StoredTabletFile,DataFileValue> files;

    SamplerConfigurationImpl samplerConfig = scanParams.getSamplerConfigurationImpl();

    synchronized (tablet) {

      if (memIters != null) {
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing memory");
      }

      if (tablet.isClosed()) {
        throw new TabletClosedException();
      }

      if (interruptFlag.get()) {
        throw new IterationInterruptedException(
            tablet.getExtent() + " " + interruptFlag.hashCode());
      }

      // only acquire the file manager when we know the tablet is open
      if (fileManager == null) {
        fileManager = tablet.getTabletResources().newScanFileManager(scanParams.getScanDispatch());
        log.trace("Adding active scan for  {}, scanId:{}", tablet.getExtent(), scanDataSourceId);
        tablet.addActiveScans(this);
      }

      if (fileManager.getNumOpenFiles() != 0) {
        throw new IllegalStateException("Tried to create new scan iterator w/o releasing files");
      }

      // set this before trying to get iterators in case
      // getIterators() throws an exception
      expectedDeletionCount = tablet.getDataSourceDeletions();

      memIters = tablet.getMemIterators(samplerConfig);
      Pair<Long,Map<StoredTabletFile,DataFileValue>> reservation = tablet.reserveFilesForScan();
      fileReservationId = reservation.getFirst();
      files = reservation.getSecond();
    }

    Collection<InterruptibleIterator> datafiles =
        fileManager.openFiles(files, scanParams.isIsolated(), samplerConfig);

    List.of(datafiles, memIters).forEach(c -> c.forEach(ii -> ii.setInterruptFlag(interruptFlag)));

    List<SortedKeyValueIterator<Key,Value>> iters =
        new ArrayList<>(datafiles.size() + memIters.size());

    iters.addAll(datafiles);
    iters.addAll(memIters);

    MultiIterator multiIter = new MultiIterator(iters, tablet.getExtent());

    var builder = new SystemIteratorEnvironmentImpl.Builder(tablet.getContext())
        .withTopLevelIterators(new ArrayList<>()).withScope(IteratorScope.scan)
        .withTableId(tablet.getExtent().tableId())
        .withAuthorizations(scanParams.getAuthorizations());
    if (samplerConfig != null) {
      builder.withSamplingEnabled();
      builder.withSamplerConfiguration(samplerConfig.toSamplerConfiguration());
    }
    SystemIteratorEnvironment iterEnv = (SystemIteratorEnvironment) builder.build();

    statsIterator = new StatsIterator(multiIter, TabletServer.seekCount, tablet.getScannedCounter(),
        tablet.getScanMetrics().getScannedCounter());

    SortedKeyValueIterator<Key,Value> visFilter =
        SystemIteratorUtil.setupSystemScanIterators(statsIterator, scanParams.getColumnSet(),
            scanParams.getAuthorizations(), defaultLabels, tablet.getTableConfiguration());

    if (loadIters) {
      List<IterInfo> iterInfos;
      Map<String,Map<String,String>> iterOpts;

      ParsedIteratorConfig pic =
          tablet.getTableConfiguration().getParsedIteratorConfig(IteratorScope.scan);
      if (scanParams.getSsiList().isEmpty() && scanParams.getSsio().isEmpty()) {
        // No scan time iterator options were set, so can just use the pre-parsed table iterator
        // options.
        iterInfos = pic.getIterInfo();
        iterOpts = pic.getOpts();
      } else {
        // Scan time iterator options were set, so need to merge those with pre-parsed table
        // iterator options.
        iterOpts = new HashMap<>(pic.getOpts().size() + scanParams.getSsio().size());
        iterInfos = new ArrayList<>(pic.getIterInfo().size() + scanParams.getSsiList().size());
        IteratorConfigUtil.mergeIteratorConfig(iterInfos, iterOpts, pic.getIterInfo(),
            pic.getOpts(), scanParams.getSsiList(), scanParams.getSsio());
      }

      String context;
      if (scanParams.getClassLoaderContext() != null) {
        log.trace("Loading iterators for scan with scan context: {}",
            scanParams.getClassLoaderContext());
        context = scanParams.getClassLoaderContext();
      } else {
        context = pic.getServiceEnv();
        if (context != null) {
          log.trace("Loading iterators for scan with table context: {}",
              scanParams.getClassLoaderContext());
        } else {
          log.trace("Loading iterators for scan");
        }
      }

      var iteratorBuilder = IteratorBuilder.builder(iterInfos).opts(iterOpts).env(iterEnv)
          .useClassLoader(context).build();
      return iterEnv
          .getTopLevelIterator(IteratorConfigUtil.loadIterators(visFilter, iteratorBuilder));
    } else {
      return visFilter;
    }
  }

  private void returnIterators() {
    if (memIters != null) {
      log.trace("Returning mem iterators for {}, scanId:{}, fid:{}", tablet.getExtent(),
          scanDataSourceId, fileReservationId);
      tablet.returnMemIterators(memIters);
      memIters = null;
      try {
        log.trace("Returning file iterators for {}, scanId:{}, fid:{}", tablet.getExtent(),
            scanDataSourceId, fileReservationId);
        tablet.returnFilesForScan(fileReservationId);
      } catch (Exception e) {
        log.warn("Error Returning file iterators for scan: {}, :{}", scanDataSourceId, e);
        // Continue bubbling the exception up for handling.
        throw e;
      } finally {
        fileReservationId = -1;
      }
    }
  }

  @Override
  public void close(boolean sawErrors) {
    try {
      returnIterators();
    } finally {
      synchronized (tablet) {
        log.trace("Removing active scan for {} scanID:{}", tablet.getExtent(), scanDataSourceId);
        if (tablet.removeScan(this) == 0) {
          tablet.notifyAll();
        }
      }
      try {
        if (fileManager != null) {
          fileManager.releaseOpenFiles(sawErrors);
        }
      } finally {
        fileManager = null;
        if (statsIterator != null) {
          statsIterator.report();
        }
      }
    }
  }

  public void interrupt() {
    interruptFlag.set(true);
  }

  @Override
  public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public void reattachFileManager() throws IOException {
    if (fileManager != null) {
      fileManager.reattach(scanParams.getSamplerConfigurationImpl());
    }
  }

  public void detachFileManager() {
    if (fileManager != null) {
      fileManager.detach();
    }
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("isNull(memIters)", memIters == null)
        .append("isNull(fileManager)", fileManager == null)
        .append("fileReservationId", fileReservationId).append("interruptFlag", interruptFlag.get())
        .append("expectedDeletionCount", expectedDeletionCount).append("scanParams", scanParams)
        .toString();
  }

  public ScanParameters getScanParameters() {
    return scanParams;
  }
}
