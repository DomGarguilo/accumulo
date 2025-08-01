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
package org.apache.accumulo.core.client;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * A scanner that instantiates iterators on the client side instead of on the tablet server. This
 * can be useful for testing iterators or in cases where you don't want iterators affecting the
 * performance of tablet servers.
 *
 * <p>
 * Suggested usage:
 *
 * <pre>
 * <code>
 * Scanner scanner = client.createScanner(tableName, authorizations);
 * scanner = new ClientSideIteratorScanner(scanner);
 * </code>
 * </pre>
 *
 * <p>
 * Iterators added to this scanner will be run in the client JVM. Separate scan iterators can be run
 * on the server side and client side by adding iterators to the source scanner (which will execute
 * server side) and to the client side scanner (which will execute client side).
 */
public class ClientSideIteratorScanner extends ScannerOptions implements Scanner {

  private int size;

  private Range range;
  private boolean isolated = false;
  private long readaheadThreshold = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD;
  private SamplerConfiguration iteratorSamplerConfig;

  private final Supplier<ClientContext> context;
  private final Supplier<TableId> tableId;

  /**
   * A class that wraps a Scanner in a SortedKeyValueIterator so that other accumulo iterators can
   * use it as a source.
   */
  private class ScannerTranslatorImpl implements SortedKeyValueIterator<Key,Value> {
    protected final Scanner scanner;
    Iterator<Entry<Key,Value>> iter;
    Entry<Key,Value> top = null;
    private SamplerConfiguration samplerConfig;

    /**
     * Constructs an accumulo iterator from a scanner.
     *
     * @param scanner the scanner to iterate over
     */
    public ScannerTranslatorImpl(final Scanner scanner, SamplerConfiguration samplerConfig) {
      this.scanner = scanner;
      this.samplerConfig = samplerConfig;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key,Value> source,
        final Map<String,String> options, final IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTop() {
      return top != null;
    }

    @Override
    public void next() throws IOException {
      if (iter.hasNext()) {
        top = iter.next();
      } else {
        top = null;
      }
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies,
        final boolean inclusive) throws IOException {
      if (!inclusive && !columnFamilies.isEmpty()) {
        throw new IllegalArgumentException();
      }
      scanner.setRange(range);
      scanner.clearColumns();
      for (ByteSequence colf : columnFamilies) {
        scanner.fetchColumnFamily(new Text(colf.toArray()));
      }

      if (samplerConfig == null) {
        scanner.clearSamplerConfiguration();
      } else {
        scanner.setSamplerConfiguration(samplerConfig);
      }

      iter = scanner.iterator();
      next();
    }

    @Override
    public Key getTopKey() {
      return top.getKey();
    }

    @Override
    public Value getTopValue() {
      return top.getValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(final IteratorEnvironment env) {
      return new ScannerTranslatorImpl(scanner,
          env.isSamplingEnabled() ? env.getSamplerConfiguration() : null);
    }
  }

  private ScannerTranslatorImpl smi;

  /**
   * Constructs a scanner that can execute client-side iterators.
   *
   * @param scanner the source scanner
   */
  public ClientSideIteratorScanner(final Scanner scanner) {
    smi = new ScannerTranslatorImpl(scanner, scanner.getSamplerConfiguration());
    this.range = scanner.getRange();
    this.size = scanner.getBatchSize();
    this.retryTimeout = scanner.getTimeout(MILLISECONDS);
    this.batchTimeout = scanner.getTimeout(MILLISECONDS);
    this.readaheadThreshold = scanner.getReadaheadThreshold();
    SamplerConfiguration samplerConfig = scanner.getSamplerConfiguration();
    if (samplerConfig != null) {
      setSamplerConfiguration(samplerConfig);
    }

    if (scanner instanceof ScannerImpl) {
      var scannerImpl = (ScannerImpl) scanner;
      this.context = () -> scannerImpl.getClientContext();
      this.tableId = () -> scannerImpl.getTableId();
    } else {
      // These may never be used, so only fail if an attempt is made to use them.
      this.context = () -> {
        throw new UnsupportedOperationException(
            "Do not know how to obtain client context from " + scanner.getClass().getName());
      };
      this.tableId = () -> {
        throw new UnsupportedOperationException(
            "Do not know how to obtain tableId from " + scanner.getClass().getName());
      };
    }
  }

  /**
   * Sets the source Scanner.
   */
  public void setSource(final Scanner scanner) {
    smi = new ScannerTranslatorImpl(scanner, scanner.getSamplerConfiguration());
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    smi.scanner.setBatchSize(size);
    smi.scanner.setTimeout(retryTimeout, MILLISECONDS);
    smi.scanner.setBatchTimeout(batchTimeout, MILLISECONDS);
    smi.scanner.setReadaheadThreshold(readaheadThreshold);
    if (isolated) {
      smi.scanner.enableIsolation();
    } else {
      smi.scanner.disableIsolation();
    }

    smi.samplerConfig = getSamplerConfiguration();

    final TreeMap<Integer,IterInfo> tm = new TreeMap<>();

    for (IterInfo iterInfo : serverSideIteratorList) {
      tm.put(iterInfo.getPriority(), iterInfo);
    }

    SortedKeyValueIterator<Key,Value> skvi;
    try {
      ClientIteratorEnvironment.Builder builder = new ClientIteratorEnvironment.Builder()
          .withClient(context.get()).withAuthorizations(getAuthorizations())
          .withScope(IteratorScope.scan).withTableId(tableId.get())
          .withSamplerConfiguration(getIteratorSamplerConfigurationInternal());
      if (getSamplerConfiguration() != null) {
        builder.withSamplingEnabled();
      }
      IteratorEnvironment iterEnv = builder.build();
      IteratorBuilder ib =
          IteratorBuilder.builder(tm.values()).opts(serverSideIteratorOptions).env(iterEnv).build();

      skvi = IteratorConfigUtil.loadIterators(smi, ib);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    final Set<ByteSequence> colfs = new TreeSet<>();
    for (Column c : this.getFetchedColumns()) {
      colfs.add(new ArrayByteSequence(c.getColumnFamily()));
    }

    try {
      skvi.seek(range, colfs, true);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new IteratorAdapter(skvi);
  }

  @Override
  public Authorizations getAuthorizations() {
    return smi.scanner.getAuthorizations();
  }

  @Override
  public void setRange(final Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(final int size) {
    this.size = size;
  }

  @Override
  public int getBatchSize() {
    return size;
  }

  @Override
  public void enableIsolation() {
    this.isolated = true;
  }

  @Override
  public void disableIsolation() {
    this.isolated = false;
  }

  @Override
  public long getReadaheadThreshold() {
    return readaheadThreshold;
  }

  @Override
  public void setReadaheadThreshold(long batches) {
    if (batches < 0) {
      throw new IllegalArgumentException(
          "Number of batches before read-ahead must be non-negative");
    }
    this.readaheadThreshold = batches;
  }

  private SamplerConfiguration getIteratorSamplerConfigurationInternal() {
    SamplerConfiguration scannerSamplerConfig = getSamplerConfiguration();
    if (scannerSamplerConfig != null) {
      if (iteratorSamplerConfig != null && !iteratorSamplerConfig.equals(scannerSamplerConfig)) {
        throw new IllegalStateException("Scanner and iterator sampler configuration differ");
      }

      return scannerSamplerConfig;
    }

    return iteratorSamplerConfig;
  }

  /**
   * This is provided for the case where no sampler configuration is set on the scanner, but there
   * is a need to create iterator deep copies that have sampling enabled. If sampler configuration
   * is set on the scanner, then this method does not need to be called inorder to create deep
   * copies with sampling.
   *
   * <p>
   * Setting this differently than the scanners sampler configuration may cause exceptions.
   *
   * @since 1.8.0
   */
  public void setIteratorSamplerConfiguration(SamplerConfiguration sc) {
    requireNonNull(sc);
    this.iteratorSamplerConfig = sc;
  }

  /**
   * Clear any iterator sampler configuration.
   *
   * @since 1.8.0
   */
  public void clearIteratorSamplerConfiguration() {
    this.iteratorSamplerConfig = null;
  }

  /**
   * @return currently set iterator sampler configuration.
   *
   * @since 1.8.0
   */

  public SamplerConfiguration getIteratorSamplerConfiguration() {
    return iteratorSamplerConfig;
  }

  @Override
  public void close() {
    smi.scanner.close();
  }
}
