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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MetadataIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(
        (cfg, coreSite) -> cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1));
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void testFlushAndCompact() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);

      // create a table to write some data to metadata table
      c.tableOperations().create(tableNames[0]);

      try (Scanner rootScanner =
          c.createScanner(SystemTables.ROOT.tableName(), Authorizations.EMPTY)) {
        rootScanner.setRange(TabletsSection.getRange());
        rootScanner.fetchColumnFamily(DataFileColumnFamily.NAME);

        Set<String> files1 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner) {
          files1.add(entry.getKey().getColumnQualifier().toString());
        }

        c.tableOperations().create(tableNames[1]);
        c.tableOperations().flush(SystemTables.METADATA.tableName(), null, null, true);

        Set<String> files2 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner) {
          files2.add(entry.getKey().getColumnQualifier().toString());
        }

        // flush of metadata table should change file set in root table
        assertTrue(!files2.isEmpty());
        assertNotEquals(files1, files2);

        c.tableOperations().compact(SystemTables.METADATA.tableName(), null, null, false, true);

        Set<String> files3 = new HashSet<>();
        for (Entry<Key,Value> entry : rootScanner) {
          files3.add(entry.getKey().getColumnQualifier().toString());
        }

        // compaction of metadata table should change file set in root table
        assertNotEquals(files2, files3);
      }
    }
  }

  @Test
  public void mergeMeta() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(5);
      SortedSet<Text> splits = new TreeSet<>();
      for (String id : "1 2 3 4 5".split(" ")) {
        splits.add(new Text(id));
      }
      c.tableOperations().addSplits(SystemTables.METADATA.tableName(), splits);
      for (String tableName : names) {
        c.tableOperations().create(tableName);
      }
      c.tableOperations().merge(SystemTables.METADATA.tableName(), null, null);
      try (Scanner s = c.createScanner(SystemTables.ROOT.tableName(), Authorizations.EMPTY)) {
        s.setRange(DeletesSection.getRange());
        while (s.stream().findAny().isEmpty()) {
          Thread.sleep(100);
        }
        assertEquals(0, c.tableOperations().listSplits(SystemTables.METADATA.tableName()).size());
      }
    }
  }

  @Test
  public void batchScanTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      // batch scan regular metadata table
      try (BatchScanner s = c.createBatchScanner(SystemTables.METADATA.tableName())) {
        s.setRanges(Collections.singleton(new Range()));
        assertTrue(s.stream().anyMatch(Objects::nonNull));
      }

      // batch scan root metadata table
      try (BatchScanner s = c.createBatchScanner(SystemTables.ROOT.tableName())) {
        s.setRanges(Collections.singleton(new Range()));
        assertTrue(s.stream().anyMatch(Objects::nonNull));
      }
    }
  }

  @Test
  public void testAmpleReadTablets() throws Exception {

    try (ClientContext cc = (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      cc.securityOperations().grantTablePermission(cc.whoami(), SystemTables.METADATA.tableName(),
          TablePermission.WRITE);

      SortedSet<Text> partitionKeys = new TreeSet<>();
      partitionKeys.add(new Text("a"));
      partitionKeys.add(new Text("e"));
      partitionKeys.add(new Text("j"));

      cc.tableOperations().create("t");
      cc.tableOperations().addSplits("t", partitionKeys);

      Text startRow = new Text("a");
      Text endRow = new Text("z");

      TabletMetadata tabletMetadata0;
      TabletMetadata tabletMetadata1;

      // Call up Ample from the client context using table "t" and build
      try (TabletsMetadata tm = cc.getAmple().readTablets().forTable(TableId.of("1"))
          .overlapping(startRow, endRow).fetch(FILES, LOCATION, LAST, PREV_ROW).build()) {
        var tablets = tm.stream().limit(2).collect(Collectors.toList());
        tabletMetadata0 = tablets.get(0);
        tabletMetadata1 = tablets.get(1);
      }

      String infoTabletId0 = tabletMetadata0.getTableId().toString();
      String infoExtent0 = tabletMetadata0.getExtent().toString();
      String infoPrevEndRow0 = tabletMetadata0.getPrevEndRow().toString();
      String infoEndRow0 = tabletMetadata0.getEndRow().toString();

      String infoTabletId1 = tabletMetadata1.getTableId().toString();
      String infoExtent1 = tabletMetadata1.getExtent().toString();
      String infoPrevEndRow1 = tabletMetadata1.getPrevEndRow().toString();
      String infoEndRow1 = tabletMetadata1.getEndRow().toString();

      String testInfoTableId = "1";

      String testInfoKeyExtent0 = "1;e;a";
      String testInfoKeyExtent1 = "1;j;e";

      String testInfoPrevEndRow0 = "a";
      String testInfoPrevEndRow1 = "e";

      String testInfoEndRow0 = "e";
      String testInfoEndRow1 = "j";

      assertEquals(infoTabletId0, testInfoTableId);
      assertEquals(infoTabletId1, testInfoTableId);

      assertEquals(infoExtent0, testInfoKeyExtent0);
      assertEquals(infoExtent1, testInfoKeyExtent1);

      assertEquals(infoPrevEndRow0, testInfoPrevEndRow0);
      assertEquals(infoPrevEndRow1, testInfoPrevEndRow1);

      assertEquals(infoEndRow0, testInfoEndRow0);
      assertEquals(infoEndRow1, testInfoEndRow1);

    }
  }

  // Test that configs related to the correctness of the Root/Metadata tables
  // are initialized correctly
  @Test
  public void testSystemTablesInitialConfigCorrectness() throws Exception {
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {

      // It is important here to use getTableProperties() and not getConfiguration()
      // because we want only the table properties and not a merged view
      var rootTableProps =
          client.tableOperations().getTableProperties(SystemTables.ROOT.tableName());
      var metadataTableProps =
          client.tableOperations().getTableProperties(SystemTables.METADATA.tableName());

      // Verify root table config
      testCommonSystemTableConfig(client, SystemTables.ROOT.tableId(), rootTableProps);

      // Verify metadata table config
      testCommonSystemTableConfig(client, SystemTables.METADATA.tableId(), metadataTableProps);
    }
  }

  private void testCommonSystemTableConfig(ClientContext client, TableId tableId,
      Map<String,String> tableProps) {
    // Verify properties all have a table. prefix
    assertTrue(tableProps.keySet().stream().allMatch(key -> key.startsWith("table.")));

    // Verify properties are correctly set
    assertEquals("5", tableProps.get(Property.TABLE_FILE_REPLICATION.getKey()));
    assertEquals("sync", tableProps.get(Property.TABLE_DURABILITY.getKey()));
    assertEquals("false", tableProps.get(Property.TABLE_FAILURES_IGNORE.getKey()));
    assertEquals("", tableProps.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey()));
    assertEquals("tablet,server", tableProps.get(Property.TABLE_LOCALITY_GROUPS.getKey()));
    assertEquals(
        String.format("%s,%s", MetadataSchema.TabletsSection.TabletColumnFamily.NAME,
            MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME),
        tableProps.get(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "tablet"));
    assertEquals(
        String.format("%s,%s,%s,%s", MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
            MetadataSchema.TabletsSection.LogColumnFamily.NAME,
            MetadataSchema.TabletsSection.ServerColumnFamily.NAME,
            MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME),
        tableProps.get(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server"));

    // Verify VersioningIterator related properties are correct
    var iterClass = "10," + VersioningIterator.class.getName();
    var maxVersions = "1";
    assertEquals(iterClass, tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers"));
    assertEquals(maxVersions,
        tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers.opt.maxVersions"));
    assertEquals(iterClass, tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers"));
    assertEquals(maxVersions,
        tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers.opt.maxVersions"));
    assertEquals(iterClass, tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers"));
    assertEquals(maxVersions,
        tableProps.get(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers.opt.maxVersions"));

    // Verify all tablets are HOSTED and initial TabletMergeability settings
    var metaSplit = MetadataSchema.TabletsSection.getRange().getEndKey().getRow();
    var initAlwaysMergeable =
        TabletMergeabilityMetadata.always(SteadyTime.from(Duration.ofMillis(0)));
    try (var tablets = client.getAmple().readTablets().forTable(tableId).build()) {
      assertTrue(
          tablets.stream().allMatch(tm -> tm.getTabletAvailability() == TabletAvailability.HOSTED));
      assertTrue(tablets.stream().allMatch(tm -> {
        // ROOT table and Metadata TabletsSection tablet should be set to never mergeable
        // All other initial tablets for Metadata, Fate, Scanref should be always
        if (SystemTables.ROOT.tableId().equals(tableId)
            || (SystemTables.METADATA.tableId().equals(tableId)
                && metaSplit.equals(tm.getEndRow()))) {
          return tm.getTabletMergeability().equals(TabletMergeabilityMetadata.never());
        }
        return tm.getTabletMergeability().equals(initAlwaysMergeable);
      }));
    }
  }
}
