/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.metrics.service.MicrometerMetricsFactory;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.easymock.EasyMock;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationMetricsTest {
  private final long currentTime = 1000L;

  /**
   * Extend the class to override the current time for testing
   */
  public class ReplicationMetricsTestMetrics extends ReplicationMetrics {
    ReplicationMetricsTestMetrics(Manager manager) {
      super(manager);
    }

    @Override
    public long getCurrentTime() {
      return currentTime;
    }
  }

  @Test
  public void testAddReplicationQueueTimeMetrics() throws Exception {
    Manager manager = EasyMock.createMock(Manager.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fileSystem = EasyMock.createMock(VolumeManager.class);
    ReplicationUtil util = EasyMock.createMock(ReplicationUtil.class);
    MutableStat stat = EasyMock.createMock(MutableStat.class);
    MutableQuantiles quantiles = EasyMock.createMock(MutableQuantiles.class);
    MicrometerMetricsFactory micrometerMF = EasyMock.createMock(MicrometerMetricsFactory.class);
    MeterRegistry meterRegistry = EasyMock.createMock(MeterRegistry.class);
    Timer timer = EasyMock.createMock(Timer.class);
    Gauge gauge = EasyMock.createMock(Gauge.class);

    Path path1 = new Path("hdfs://localhost:9000/accumulo/wal/file1");
    Path path2 = new Path("hdfs://localhost:9000/accumulo/wal/file2");

    // First call will initialize the map of paths to modification time
    EasyMock.expect(manager.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(manager.getMicrometerMetrics()).andReturn(micrometerMF).anyTimes();
    EasyMock.expect(micrometerMF.getRegistry()).andReturn(meterRegistry).anyTimes();
    EasyMock.expect(meterRegistry.timer("replicationQueue")).andReturn(timer).anyTimes();
    EasyMock.expect(meterRegistry.gauge("filesPendingReplication", new AtomicLong(0L),
            AtomicLong::get)).andReturn(new AtomicLong(0L));
    //EasyMock.expect(meterRegistry.gauge("filesPendingReplication", new AtomicLong(0L)))
    // .andReturn(new AtomicLong(0L)).anyTimes();
    EasyMock.expect(meterRegistry.gauge("numPeers", new AtomicInteger(0))).andReturn(new AtomicInteger(0)).anyTimes();
    EasyMock.expect(meterRegistry.gauge("maxReplicationThreads", new AtomicInteger(0))).andReturn(new AtomicInteger(0)).anyTimes();
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(Set.of(path1, path2));
    EasyMock.expect(manager.getVolumeManager()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path1)).andReturn(createStatus(100));
    EasyMock.expect(manager.getVolumeManager()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path2)).andReturn(createStatus(200));

    // Second call will recognize the missing path1 and add the latency stat
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(Set.of(path2));

    // Expect a call to reset the min/max
    stat.resetMinMax();
    EasyMock.expectLastCall();

    // Expect the calls of adding the stats
    quantiles.add(currentTime - 100);
    EasyMock.expectLastCall();

    stat.add(currentTime - 100);
    EasyMock.expectLastCall();

    // timer.record(Duration.ofMillis(currentTime - 100));
    // EasyMock.expectLastCall();

    EasyMock.replay(manager, fileSystem, util, stat, quantiles,micrometerMF,meterRegistry);//,
    // timer);

    ReplicationMetrics metrics = new ReplicationMetricsTestMetrics(manager);

    // Inject our mock objects
    replaceField(metrics, "replicationUtil", util);
    replaceField(metrics, "replicationQueueTimeQuantiles", quantiles);
    replaceField(metrics, "replicationQueueTimeStat", stat);

    // Two calls to this will initialize the map and then add metrics
    metrics.addReplicationQueueTimeMetrics();
    metrics.addReplicationQueueTimeMetrics();

    EasyMock.verify(manager, fileSystem, util, stat, quantiles,micrometerMF,meterRegistry);//,
    // timer);
  }

  private void replaceField(Object instance, String fieldName, Object target)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = instance.getClass().getSuperclass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(instance, target);
  }

  private FileStatus createStatus(long modtime) {
    return new FileStatus(0, false, 0, 0, modtime, 0, null, null, null, null);
  }
}
