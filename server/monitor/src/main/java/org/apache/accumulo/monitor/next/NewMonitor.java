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
package org.apache.accumulo.monitor.next;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.EnumSet;

import jakarta.ws.rs.NotFoundException;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.monitor.next.serializers.CumulativeDistributionSummarySerializer;
import org.apache.accumulo.monitor.next.serializers.MetricResponseSerializer;
import org.apache.accumulo.monitor.next.serializers.ThriftSerializer;
import org.apache.accumulo.server.ServerContext;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ConnectionStatistics;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.module.SimpleModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import io.javalin.security.RouteRole;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

public class NewMonitor implements Connection.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(NewMonitor.class);

  private static final EnumSet<Property> requireForSecure =
      EnumSet.of(Property.MONITOR_SSL_KEYSTORE, Property.MONITOR_SSL_KEYSTOREPASS,
          Property.MONITOR_SSL_TRUSTSTORE, Property.MONITOR_SSL_TRUSTSTOREPASS);

  public static final int NEW_MONITOR_PORT = 43331;

  private final ServerContext ctx;
  private final String hostname;
  private final boolean secure;
  private final ConnectionStatistics connStats;
  private final InformationFetcher metrics;

  public NewMonitor(ServerContext ctx, String hostname) {
    this.ctx = ctx;
    this.hostname = hostname;
    this.secure = requireForSecure.stream().map(ctx.getConfiguration()::get)
        .allMatch(s -> s != null && !s.isEmpty());

    this.connStats = new ConnectionStatistics();
    this.metrics = new InformationFetcher(ctx, connStats::getConnections);
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET",
      justification = "TODO Replace before merging")
  public void start() throws IOException {

    // Find a free socket
    ServerSocket ss = new ServerSocket();
    ss.setReuseAddress(true);
    ss.bind(new InetSocketAddress(hostname, NEW_MONITOR_PORT));
    ss.close();
    final int httpPort = ss.getLocalPort();

    Threads.createThread("Metric Fetcher Thread", metrics).start();

    Javalin.create(config -> {
      // TODO Make dev logging and route overview configurable based on property
      // They are useful for development and debugging, but should probably not
      // be enabled for normal use.
      config.bundledPlugins.enableDevLogging();
      config.bundledPlugins.enableRouteOverview("/routes", new RouteRole[] {});
      config.jsonMapper(new JavalinJackson().updateMapper(mapper -> {
        SimpleModule module = new SimpleModule();
        module.addSerializer(MetricResponse.class, new MetricResponseSerializer());
        module.addSerializer(TExternalCompaction.class, new ThriftSerializer());
        module.addSerializer(TExternalCompactionJob.class, new ThriftSerializer());
        module.addSerializer(CumulativeDistributionSummary.class,
            new CumulativeDistributionSummarySerializer());
        mapper.registerModule(module);
      }));

      final HttpConnectionFactory httpFactory = new HttpConnectionFactory();

      // Set up TLS
      if (secure) {
        LOG.debug("Configuring Jetty to use TLS");

        final AccumuloConfiguration conf = ctx.getConfiguration();

        final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        // If the key password is the same as the keystore password, we don't
        // have to explicitly set it. Thus, if the user doesn't provide a key
        // password, don't set anything.
        final String keyPass = conf.get(Property.MONITOR_SSL_KEYPASS);
        if (!Property.MONITOR_SSL_KEYPASS.getDefaultValue().equals(keyPass)) {
          sslContextFactory.setKeyManagerPassword(keyPass);
        }
        sslContextFactory.setKeyStorePath(conf.get(Property.MONITOR_SSL_KEYSTORE));
        sslContextFactory.setKeyStorePassword(conf.get(Property.MONITOR_SSL_KEYSTOREPASS));
        sslContextFactory.setKeyStoreType(conf.get(Property.MONITOR_SSL_KEYSTORETYPE));
        sslContextFactory.setTrustStorePath(conf.get(Property.MONITOR_SSL_TRUSTSTORE));
        sslContextFactory.setTrustStorePassword(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS));
        sslContextFactory.setTrustStoreType(conf.get(Property.MONITOR_SSL_TRUSTSTORETYPE));

        final String includedCiphers = conf.get(Property.MONITOR_SSL_INCLUDE_CIPHERS);
        if (!Property.MONITOR_SSL_INCLUDE_CIPHERS.getDefaultValue().equals(includedCiphers)) {
          sslContextFactory.setIncludeCipherSuites(includedCiphers.split(","));
        }

        final String excludedCiphers = conf.get(Property.MONITOR_SSL_EXCLUDE_CIPHERS);
        if (!Property.MONITOR_SSL_EXCLUDE_CIPHERS.getDefaultValue().equals(excludedCiphers)) {
          sslContextFactory.setExcludeCipherSuites(excludedCiphers.split(","));
        }

        final String includeProtocols = conf.get(Property.MONITOR_SSL_INCLUDE_PROTOCOLS);
        if (includeProtocols != null && !includeProtocols.isEmpty()) {
          sslContextFactory.setIncludeProtocols(includeProtocols.split(","));
        }

        final SslConnectionFactory sslFactory =
            new SslConnectionFactory(sslContextFactory, httpFactory.getProtocol());
        config.jetty.addConnector((s, httpConfig) -> {
          ServerConnector conn = new ServerConnector(s, sslFactory, httpFactory);
          // Capture connection statistics
          conn.addBean(connStats);
          // Listen for connection events
          conn.addBean(this);
          conn.setHost(hostname);
          conn.setPort(httpPort);
          return conn;
        });
      } else {
        config.jetty.addConnector((s, httpConfig) -> {
          ServerConnector conn = new ServerConnector(s, httpFactory);
          // Capture connection statistics
          conn.addBean(connStats);
          // Listen for connection events
          conn.addBean(this);
          conn.setHost(hostname);
          conn.setPort(httpPort);
          return conn;
        });
      }
    }).get("/stats", ctx -> ctx.result(connStats.dump()))
        .get("/metrics", ctx -> ctx.json(metrics.getAll()))
        .get("/metrics/instance", ctx -> ctx.json(metrics.getInstanceSummary()))
        .get("/metrics/groups", ctx -> ctx.json(metrics.getResourceGroups()))
        .get("/metrics/manager", ctx -> ctx.json(metrics.getManager()))
        .get("/metrics/gc", ctx -> ctx.json(metrics.getGarbageCollector()))
        .get("/metrics/compactors/summary", ctx -> ctx.json(metrics.getCompactorAllMetricSummary()))
        .get("/metrics/compactors/summary/{group}",
            ctx -> ctx.json(metrics.getCompactorResourceGroupMetricSummary(ctx.pathParam("group"))))
        .get("/metrics/compactors/detail/{group}",
            ctx -> ctx.json(metrics.getCompactors(ctx.pathParam("group"))))
        .get("/metrics/sservers/summary", ctx -> ctx.json(metrics.getScanServerAllMetricSummary()))
        .get("/metrics/sservers/summary/{group}",
            ctx -> ctx
                .json(metrics.getScanServerResourceGroupMetricSummary(ctx.pathParam("group"))))
        .get("/metrics/sservers/detail/{group}",
            ctx -> ctx.json(metrics.getScanServers(ctx.pathParam("group"))))
        .get("/metrics/tservers/summary",
            ctx -> ctx.json(metrics.getTabletServerAllMetricSummary()))
        .get("/metrics/tservers/summary/{group}",
            ctx -> ctx
                .json(metrics.getTabletServerResourceGroupMetricSummary(ctx.pathParam("group"))))
        .get("/metrics/tservers/detail/{group}",
            ctx -> ctx.json(metrics.getTabletServers(ctx.pathParam("group"))))
        .get("/metrics/problems", ctx -> ctx.json(metrics.getProblemHosts()))
        .get("/metrics/compactions", ctx -> ctx.json(metrics.getCompactions(25)))
        .get("/metrics/compactions/{num}",
            ctx -> ctx.json(metrics.getCompactions(Integer.parseInt(ctx.pathParam("num")))))
        .exception(NotFoundException.class, (e, ctx) -> ctx.status(404)).start();

    LOG.info("New Monitor listening on port: {}", httpPort);
  }

  @Override
  public void onOpened(Connection connection) {
    LOG.info("New connection event");
    metrics.newConnectionEvent();
  }

  @Override
  public void onClosed(Connection connection) {
    // do nothing
  }

}
