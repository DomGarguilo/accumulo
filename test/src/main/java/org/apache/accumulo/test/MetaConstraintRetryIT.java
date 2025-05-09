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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.upgrade.SplitRecovery12to13;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

public class MetaConstraintRetryIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  // a test for ACCUMULO-3096
  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().grantTablePermission(getAdminPrincipal(),
          SystemTables.METADATA.tableName(), TablePermission.WRITE);

      ServerContext context = getServerContext();
      KeyExtent extent = new KeyExtent(TableId.of("5"), null, null);

      Mutation m = new Mutation(extent.toMetaRow());
      // unknown columns should cause constraint violation
      m.put("badcolfam", "badcolqual", "3");
      var iae = assertThrows(IllegalArgumentException.class,
          () -> SplitRecovery12to13.update(context, null, m, extent));
      assertEquals(MutationsRejectedException.class, iae.getCause().getClass());
      var mre = (MutationsRejectedException) iae.getCause();
      assertFalse(mre.getConstraintViolationSummaries().isEmpty());
    }
  }
}
