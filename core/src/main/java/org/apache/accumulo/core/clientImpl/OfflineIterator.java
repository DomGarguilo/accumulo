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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

class OfflineIterator implements Iterator<Entry<Key,Value>> {

  private SortedKeyValueIterator<Key,Value> iter;
  private Range range;
  private KeyExtent currentExtent;
  private final TableId tableId;
  private final Authorizations authorizations;
  private final ClientContext context;
  private final ScannerOptions options;
  private final ArrayList<SortedKeyValueIterator<Key,Value>> readers;

  public OfflineIterator(ScannerOptions options, ClientContext context,
      Authorizations authorizations, Text table, Range range) {
    this.options = new ScannerOptions(options);
    this.context = context;
    this.range = range;

    if (!this.options.fetchedColumns.isEmpty()) {
      this.range =
          range.bound(this.options.fetchedColumns.first(), this.options.fetchedColumns.last());
    }

    this.tableId = TableId.of(table.toString());
    this.authorizations = authorizations;
    this.readers = new ArrayList<>();

    try {
      nextTablet();

      while (iter != null && !iter.hasTop()) {
        nextTablet();
      }

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return iter != null && iter.hasTop();
  }

  @Override
  public Entry<Key,Value> next() {
    try {
      byte[] v = iter.getTopValue().get();
      // copy just like tablet server does, do this before calling next
      KeyValue ret = new KeyValue(new Key(iter.getTopKey()), Arrays.copyOf(v, v.length));

      iter.next();

      while (iter != null && !iter.hasTop()) {
        nextTablet();
      }

      return ret;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private void nextTablet() throws TableNotFoundException, AccumuloException, IOException,
      AccumuloSecurityException, ReflectiveOperationException {

    Range nextRange;

    if (currentExtent == null) {
      Text startRow;

      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      nextRange = new Range(TabletsSection.encodeRow(tableId, startRow), true, null, false);
    } else {

      if (currentExtent.endRow() == null
          || range.afterEndKey(new Key(currentExtent.endRow()).followingKey(PartialKey.ROW))) {
        iter = null;
        return;
      }

      nextRange = new Range(currentExtent.toMetaRow(), false, null, false);
    }

    TabletMetadata tablet = getTabletFiles(nextRange);

    while (tablet.getLocation() != null) {
      if (context.getTableState(tableId) != TableState.OFFLINE) {
        context.clearTableListCache();
        if (context.getTableState(tableId) != TableState.OFFLINE) {
          throw new AccumuloException("Table is online " + tableId
              + " cannot scan tablet in offline mode " + tablet.getExtent());
        }
      }

      sleepUninterruptibly(250, MILLISECONDS);

      tablet = getTabletFiles(nextRange);
    }

    if (!tablet.getExtent().tableId().equals(tableId)) {
      throw new AccumuloException(
          " did not find tablets for table " + tableId + " " + tablet.getExtent());
    }

    if (currentExtent != null && !tablet.getExtent().isPreviousExtent(currentExtent)) {
      throw new AccumuloException(
          " " + currentExtent + " is not previous extent " + tablet.getExtent());
    }

    iter = createIterator(tablet.getExtent(), tablet.getFiles());
    iter.seek(range, LocalityGroupUtil.families(options.fetchedColumns),
        !options.fetchedColumns.isEmpty());
    currentExtent = tablet.getExtent();

  }

  private TabletMetadata getTabletFiles(Range nextRange) {
    try (TabletsMetadata tablets = TabletsMetadata.builder(context).scanMetadataTable()
        .overRange(nextRange).fetch(FILES, LOCATION, PREV_ROW).build()) {
      return tablets.iterator().next();
    }
  }

  private SortedKeyValueIterator<Key,Value> createIterator(KeyExtent extent,
      Collection<StoredTabletFile> absFiles) throws TableNotFoundException, AccumuloException,
      IOException, AccumuloSecurityException, ReflectiveOperationException {

    // possible race condition here, if table is renamed
    String tableName = context.getQualifiedTableName(tableId);
    var tableConf = context.tableOperations().getConfiguration(tableName);
    AccumuloConfiguration tableCC = new ConfigurationCopy(tableConf);
    var systemConf = context.instanceOperations().getSystemConfiguration();

    Configuration conf = context.getHadoopConf();

    for (SortedKeyValueIterator<Key,Value> reader : readers) {
      ((FileSKVIterator) reader).close();
    }

    readers.clear();

    SamplerConfiguration scannerSamplerConfig = options.getSamplerConfiguration();
    SamplerConfigurationImpl scannerSamplerConfigImpl =
        scannerSamplerConfig == null ? null : new SamplerConfigurationImpl(scannerSamplerConfig);
    SamplerConfigurationImpl samplerConfImpl = SamplerConfigurationImpl.newSamplerConfig(tableCC);

    if (scannerSamplerConfigImpl != null && !scannerSamplerConfigImpl.equals(samplerConfImpl)) {
      throw new SampleNotPresentException();
    }
    for (StoredTabletFile file : absFiles) {
      var cs = CryptoFactoryLoader.getServiceForClientWithTable(systemConf, tableConf, tableId);
      FileSystem fs = VolumeConfiguration.fileSystemForPath(file.getNormalizedPathStr(), conf);
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(file, fs, conf, cs).withTableConfiguration(tableCC).build();
      if (scannerSamplerConfigImpl != null) {
        reader = reader.getSample(scannerSamplerConfigImpl);
        if (reader == null) {
          throw new SampleNotPresentException();
        }
      }
      readers.add(reader);
    }

    MultiIterator multiIter = new MultiIterator(readers, extent);

    ClientIteratorEnvironment.Builder iterEnvBuilder =
        new ClientIteratorEnvironment.Builder().withAuthorizations(authorizations)
            .withScope(IteratorScope.scan).withTableId(tableId).withClient(context);
    if (samplerConfImpl != null) {
      iterEnvBuilder.withSamplerConfiguration(samplerConfImpl.toSamplerConfiguration());
    }
    IteratorEnvironment iterEnv = iterEnvBuilder.build();

    byte[] defaultSecurityLabel;
    ColumnVisibility cv =
        new ColumnVisibility(tableCC.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
    defaultSecurityLabel = cv.getExpression();

    SortedKeyValueIterator<Key,Value> visFilter =
        SystemIteratorUtil.setupSystemScanIterators(multiIter,
            new HashSet<>(options.fetchedColumns), authorizations, defaultSecurityLabel, tableCC);
    var iteratorBuilderEnv = IteratorConfigUtil.loadIterConf(IteratorScope.scan,
        options.serverSideIteratorList, options.serverSideIteratorOptions, tableCC);
    var iteratorBuilder = iteratorBuilderEnv.env(iterEnv).build();
    return IteratorConfigUtil.loadIterators(visFilter, iteratorBuilder);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
