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
package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFileScannerBuilder.InputArgs;
import org.apache.accumulo.core.clientImpl.ClientServiceEnvironmentImpl;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.impl.NoopCache;
import org.apache.accumulo.core.file.blockfile.impl.BasicCacheProvider;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

class RFileScanner extends ScannerOptions implements Scanner {

  private static class RFileScannerEnvironmentImpl extends ClientServiceEnvironmentImpl {

    private final Configuration conf;
    private final Configuration tableConf;

    public RFileScannerEnvironmentImpl(Opts opts) {
      super(null);
      conf = new ConfigurationImpl(new ConfigurationCopy(DefaultConfiguration.getInstance()));
      ConfigurationCopy tableCC = new ConfigurationCopy(DefaultConfiguration.getInstance());
      if (opts.tableConfig != null) {
        opts.tableConfig.forEach(tableCC::set);
      }
      tableConf = new ConfigurationImpl(tableCC);
    }

    @Override
    public String getTableName(TableId tableId) throws TableNotFoundException {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public Configuration getConfiguration(TableId tableId) {
      return tableConf;
    }

  }

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final Range EMPTY_RANGE = new Range();

  private Range range;
  private BlockCacheManager blockCacheManager = null;
  private BlockCache dataCache = null;
  private BlockCache indexCache = null;
  private Opts opts;
  private int batchSize = 1000;
  private long readaheadThreshold = 3;
  private AccumuloConfiguration tableConf;
  private CryptoService cryptoService;

  static class Opts {
    InputArgs in;
    Authorizations auths = Authorizations.EMPTY;
    long dataCacheSize;
    long indexCacheSize;
    boolean useSystemIterators = true;
    public HashMap<String,String> tableConfig;
    Range bounds;
  }

  RFileScanner(Opts opts) {
    if (!opts.auths.equals(Authorizations.EMPTY) && !opts.useSystemIterators) {
      throw new IllegalArgumentException(
          "Set authorizations and specified not to use system iterators");
    }

    this.opts = opts;
    if (opts.tableConfig != null && !opts.tableConfig.isEmpty()) {
      ConfigurationCopy tableCC = new ConfigurationCopy(DefaultConfiguration.getInstance());
      opts.tableConfig.forEach(tableCC::set);
      this.tableConf = tableCC;
    } else {
      this.tableConf = DefaultConfiguration.getInstance();
    }

    if (opts.indexCacheSize > 0 || opts.dataCacheSize > 0) {
      ConfigurationCopy cc = tableConf instanceof ConfigurationCopy ? (ConfigurationCopy) tableConf
          : new ConfigurationCopy(tableConf);
      try {
        blockCacheManager = BlockCacheManagerFactory.getClientInstance(cc);
        if (opts.indexCacheSize > 0) {
          cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(opts.indexCacheSize));
        }
        if (opts.dataCacheSize > 0) {
          cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(opts.dataCacheSize));
        }
        blockCacheManager.start(BlockCacheConfiguration.forTabletServer(cc));
        this.indexCache = blockCacheManager.getBlockCache(CacheType.INDEX);
        this.dataCache = blockCacheManager.getBlockCache(CacheType.DATA);
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException(
            "Configuration does not contain loadable class for block cache manager factory", e);
      }
    }
    if (indexCache == null) {
      this.indexCache = new NoopCache();
    }
    if (this.dataCache == null) {
      this.dataCache = new NoopCache();
    }
    this.cryptoService =
        CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, opts.tableConfig);
  }

  @Override
  public synchronized void fetchColumnFamily(Text col) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumnFamily(col);
  }

  @Override
  public synchronized void fetchColumn(Text colFam, Text colQual) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumn(colFam, colQual);
  }

  @Override
  public void fetchColumn(IteratorSetting.Column column) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumn(column);
  }

  @Override
  public void setClassLoaderContext(String classLoaderContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRange(Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void enableIsolation() {}

  @Override
  public void disableIsolation() {}

  @Override
  public synchronized void setReadaheadThreshold(long batches) {
    Preconditions.checkArgument(batches > 0);
    readaheadThreshold = batches;
  }

  @Override
  public synchronized long getReadaheadThreshold() {
    return readaheadThreshold;
  }

  @Override
  public Authorizations getAuthorizations() {
    return opts.auths;
  }

  @Override
  public void addScanIterator(IteratorSetting cfg) {
    super.addScanIterator(cfg);
  }

  @Override
  public void removeScanIterator(String iteratorName) {
    super.removeScanIterator(iteratorName);
  }

  @Override
  public void updateScanIteratorOption(String iteratorName, String key, String value) {
    super.updateScanIteratorOption(iteratorName, key, value);
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    try {
      RFileSource[] sources = opts.in.getSources();
      List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(sources.length);

      CacheProvider cacheProvider = new BasicCacheProvider(indexCache, dataCache);

      for (int i = 0; i < sources.length; i++) {
        // TODO may have been a bug with multiple files and caching in older version...
        CachableBuilder cb = new CachableBuilder()
            .input((FSDataInputStream) sources[i].getInputStream(), "source-" + i)
            .length(sources[i].getLength()).conf(opts.in.getConf()).cacheProvider(cacheProvider)
            .cryptoService(cryptoService);
        readers.add(RFile.getReader(cb, sources[i].getRange()));
      }

      if (getSamplerConfiguration() != null) {
        for (int i = 0; i < readers.size(); i++) {
          readers.set(i, ((Reader) readers.get(i))
              .getSample(new SamplerConfigurationImpl(getSamplerConfiguration())));
        }
      }

      SortedKeyValueIterator<Key,Value> iterator;
      if (opts.bounds != null) {
        iterator = new MultiIterator(readers, opts.bounds);
      } else {
        iterator = new MultiIterator(readers, false);
      }

      Set<ByteSequence> families = Collections.emptySet();

      if (opts.useSystemIterators) {
        SortedSet<Column> cols = this.getFetchedColumns();
        families = LocalityGroupUtil.families(cols);
        iterator = SystemIteratorUtil.setupSystemScanIterators(iterator, cols, getAuthorizations(),
            EMPTY_BYTES, tableConf);
      }

      ClientIteratorEnvironment.Builder iterEnvBuilder = new ClientIteratorEnvironment.Builder()
          .withEnvironment(new RFileScannerEnvironmentImpl(opts)).withAuthorizations(opts.auths)
          .withScope(IteratorScope.scan).withTableId(null);
      if (getSamplerConfiguration() != null) {
        iterEnvBuilder.withSamplerConfiguration(getSamplerConfiguration());
        iterEnvBuilder.withSamplingEnabled();
      }
      IteratorEnvironment iterEnv = iterEnvBuilder.build();
      try {
        if (opts.tableConfig != null && !opts.tableConfig.isEmpty()) {
          var ibEnv = IteratorConfigUtil.loadIterConf(IteratorScope.scan, serverSideIteratorList,
              serverSideIteratorOptions, tableConf);
          var iteratorBuilder = ibEnv.env(iterEnv).build();
          iterator = IteratorConfigUtil.loadIterators(iterator, iteratorBuilder);
        } else {
          var iteratorBuilder = IteratorBuilder.builder(serverSideIteratorList)
              .opts(serverSideIteratorOptions).env(iterEnv).build();
          iterator = IteratorConfigUtil.loadIterators(iterator, iteratorBuilder);
        }
      } catch (IOException | ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }

      iterator.seek(getRange() == null ? EMPTY_RANGE : getRange(), families, !families.isEmpty());
      return new IteratorAdapter(iterator);

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      for (RFileSource source : opts.in.getSources()) {
        source.getInputStream().close();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (this.blockCacheManager != null) {
      this.blockCacheManager.stop();
    }
  }
}
