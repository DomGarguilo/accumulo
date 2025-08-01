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
package org.apache.accumulo.manager.tableOps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ShutdownTServerTest {

  @Test
  public void testSingleShutdown() throws Exception {
    HostAndPort hap = HostAndPort.fromParts("localhost", 1234);
    final TServerInstance tserver = new TServerInstance(hap, "fake");
    final boolean force = false;

    final ShutdownTServer op = new ShutdownTServer(tserver, ResourceGroupId.DEFAULT, force);

    final Manager manager = EasyMock.createMock(Manager.class);
    final FateId fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());

    final TServerConnection tserverCnxn = EasyMock.createMock(TServerConnection.class);
    final TabletServerStatus status = new TabletServerStatus();
    status.tableMap = new HashMap<>();
    // Put in a table info record, don't care what
    status.tableMap.put("a_table", new TableInfo());

    EasyMock.expect(manager.shutdownTServer(tserver)).andReturn(true).once();
    EasyMock.expect(manager.onlineTabletServers()).andReturn(Collections.singleton(tserver));
    EasyMock.expect(manager.getConnection(tserver)).andReturn(tserverCnxn);
    EasyMock.expect(tserverCnxn.getTableMap(false)).andReturn(status);

    EasyMock.replay(tserverCnxn, manager);

    // FATE op is not ready
    long wait = op.isReady(fateId, manager);
    assertTrue(wait > 0, "Expected wait to be greater than 0");

    EasyMock.verify(tserverCnxn, manager);

    // Reset the mocks
    EasyMock.reset(tserverCnxn, manager);

    // reset the table map to the empty set to simulate all tablets unloaded
    status.tableMap = new HashMap<>();
    EasyMock.expect(manager.shutdownTServer(tserver)).andReturn(false).once();
    EasyMock.expect(manager.onlineTabletServers()).andReturn(Collections.singleton(tserver));
    EasyMock.expect(manager.getConnection(tserver)).andReturn(tserverCnxn);
    EasyMock.expect(tserverCnxn.getTableMap(false)).andReturn(status);
    EasyMock.expect(manager.getManagerLock()).andReturn(null);
    tserverCnxn.halt(null);
    EasyMock.expectLastCall().once();

    EasyMock.replay(tserverCnxn, manager);

    // FATE op is not ready
    wait = op.isReady(fateId, manager);
    assertEquals(0, wait, "Expected wait to be 0");

    Repo<Manager> op2 = op.call(fateId, manager);
    assertNull(op2, "Expected no follow on step");

    EasyMock.verify(tserverCnxn, manager);
  }

}
