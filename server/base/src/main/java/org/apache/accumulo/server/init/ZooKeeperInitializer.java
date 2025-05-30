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
package org.apache.accumulo.server.init;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.clientImpl.TabletAvailabilityUtil;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.util.tables.TableMapping;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.metadata.RootGcCandidates;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class ZooKeeperInitializer {

  private final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private final byte[] ZERO_CHAR_ARRAY = {'0'};

  /**
   * The prop store requires that the system config node exists so that it can set watchers. This
   * methods creates the ZooKeeper nodes /accumulo/INSTANCE_ID/config/encoded_props and sets the
   * encoded_props to a default, empty node if it does not exist. If any of the paths or the
   * encoded_props node exist, the are skipped and not modified.
   *
   * @param instanceId the instance id
   * @param zoo a ZooReaderWriter
   */
  void initializeConfig(final InstanceId instanceId, final ZooReaderWriter zoo) {
    try {

      zoo.putPersistentData(Constants.ZROOT, new byte[0], ZooUtil.NodeExistsPolicy.SKIP,
          ZooDefs.Ids.OPEN_ACL_UNSAFE);

      String zkInstanceRoot = ZooUtil.getRoot(instanceId);
      zoo.putPersistentData(zkInstanceRoot, EMPTY_BYTE_ARRAY, ZooUtil.NodeExistsPolicy.SKIP);
      var sysPropPath = SystemPropKey.of().getPath();
      VersionedProperties vProps = new VersionedProperties();
      // skip if the encoded props node exists
      if (zoo.exists(sysPropPath)) {
        return;
      }
      var created = zoo.putPrivatePersistentData(zkInstanceRoot + sysPropPath,
          VersionedPropCodec.getDefault().toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
      if (!created) {
        throw new IllegalStateException(
            "Failed to create default system props during initialization at: {}" + sysPropPath);
      }
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to initialize configuration for prop store", ex);
    }
  }

  void initialize(final ServerContext context, final String rootTabletDirName,
      final String rootTabletFileUri) throws KeeperException, InterruptedException {
    ZooReaderWriter zrwChroot = context.getZooSession().asReaderWriter();
    zrwChroot.putPersistentData(Constants.ZTABLES, Constants.ZTABLES_INITIAL_ID,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZNAMESPACES,
        NamespaceMapping
            .serializeMap(Map.of(Namespace.DEFAULT.id().canonical(), Namespace.DEFAULT.name(),
                Namespace.ACCUMULO.id().canonical(), Namespace.ACCUMULO.name())),
        ZooUtil.NodeExistsPolicy.FAIL);

    context.getTableManager().prepareNewNamespaceState(Namespace.DEFAULT.id(),
        Namespace.DEFAULT.name(), ZooUtil.NodeExistsPolicy.FAIL);
    context.getTableManager().prepareNewNamespaceState(Namespace.ACCUMULO.id(),
        Namespace.ACCUMULO.name(), ZooUtil.NodeExistsPolicy.FAIL);

    zrwChroot.putPersistentData(TableMapping.getZTableMapPath(Namespace.ACCUMULO.id()),
        NamespaceMapping.serializeMap(SystemTables.tableIdToSimpleNameMap()),
        ZooUtil.NodeExistsPolicy.OVERWRITE);

    context.getTableManager().prepareNewTableState(SystemTables.ROOT.tableId(),
        Namespace.ACCUMULO.id(), SystemTables.ROOT.tableName(), TableState.ONLINE,
        ZooUtil.NodeExistsPolicy.FAIL);
    context.getTableManager().prepareNewTableState(SystemTables.METADATA.tableId(),
        Namespace.ACCUMULO.id(), SystemTables.METADATA.tableName(), TableState.ONLINE,
        ZooUtil.NodeExistsPolicy.FAIL);
    // Call this separately so the upgrader code can handle the zk node creation for scan refs
    initScanRefTableState(context);
    initFateTableState(context);

    zrwChroot.putPersistentData(Constants.ZTSERVERS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(RootTable.ZROOT_TABLET,
        getInitialRootTabletJson(rootTabletDirName, rootTabletFileUri),
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(RootTable.ZROOT_TABLET_GC_CANDIDATES,
        new RootGcCandidates().toJson().getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZMANAGERS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZMANAGER_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZMANAGER_GOAL_STATE,
        ManagerGoalState.NORMAL.toString().getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZGC, EMPTY_BYTE_ARRAY, ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZGC_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZTABLE_LOCKS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZHDFS_RESERVATIONS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZNEXT_FILE, ZERO_CHAR_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZRECOVERY, ZERO_CHAR_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZMONITOR, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZMONITOR_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(WalStateManager.ZWALS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZCOMPACTORS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZSSERVERS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zrwChroot.putPersistentData(Constants.ZCOMPACTIONS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
  }

  /**
   * Generate initial json for the root tablet metadata. Return the JSON converted to a byte[].
   */
  public static byte[] getInitialRootTabletJson(String dirName, String file) {
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(dirName);
    Mutation mutation =
        MetadataSchema.TabletsSection.TabletColumnFamily.createPrevRowMutation(RootTable.EXTENT);
    MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mutation,
        new Value(dirName));

    mutation.put(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME,
        StoredTabletFile.serialize(file), new DataFileValue(0, 0).encodeAsValue());

    MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mutation,
        new Value(new MetadataTime(0, TimeType.LOGICAL).encode()));

    MetadataSchema.TabletsSection.TabletColumnFamily.AVAILABILITY_COLUMN.put(mutation,
        TabletAvailabilityUtil.toValue(TabletAvailability.HOSTED));

    RootTabletMetadata rootTabletJson = new RootTabletMetadata();
    rootTabletJson.update(mutation);

    return rootTabletJson.toJson().getBytes(UTF_8);
  }

  public void initScanRefTableState(ServerContext context) {
    try {
      context.getTableManager().prepareNewTableState(SystemTables.SCAN_REF.tableId(),
          Namespace.ACCUMULO.id(), SystemTables.SCAN_REF.tableName(), TableState.ONLINE,
          ZooUtil.NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void initFateTableState(ServerContext context) {
    try {
      context.getTableManager().prepareNewTableState(SystemTables.FATE.tableId(),
          Namespace.ACCUMULO.id(), SystemTables.FATE.tableName(), TableState.ONLINE,
          ZooUtil.NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void initInstanceNameAndId(ZooReaderWriter zoo, InstanceId instanceId,
      final boolean clearInstanceName, final String instanceNamePath)
      throws InterruptedException, KeeperException {

    // setup basic data in zookeeper
    zoo.putPersistentData(Constants.ZROOT + Constants.ZINSTANCES, new byte[0],
        ZooUtil.NodeExistsPolicy.SKIP, ZooDefs.Ids.OPEN_ACL_UNSAFE);

    // setup instance name
    if (clearInstanceName) {
      zoo.recursiveDelete(instanceNamePath, ZooUtil.NodeMissingPolicy.SKIP);
    }
    zoo.putPersistentData(instanceNamePath, instanceId.canonical().getBytes(UTF_8),
        ZooUtil.NodeExistsPolicy.FAIL);
  }

}
