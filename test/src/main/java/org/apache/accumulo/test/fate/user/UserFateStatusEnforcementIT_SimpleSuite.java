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
package org.apache.accumulo.test.fate.user;

import static org.apache.accumulo.test.fate.FateTestUtil.createFateTable;
import static org.apache.accumulo.test.fate.TestLock.createDummyLockID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateStatusEnforcementITBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class UserFateStatusEnforcementIT_SimpleSuite extends FateStatusEnforcementITBase {
  private ClientContext client;
  private String table;

  @BeforeAll
  public static void beforeAllSetup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void afterAllTeardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @BeforeEach
  public void beforeEachSetup() throws Exception {
    client = (ClientContext) Accumulo.newClient().from(getClientProps()).build();
    table = getUniqueNames(1)[0];
    createFateTable(client, table);
    store = new UserFateStore<>(client, table, createDummyLockID(), null);
    fateId = store.create();
    txStore = store.reserve(fateId);
  }

  @AfterEach
  public void afterEachTeardown() {
    store.close();
    client.close();
  }
}
