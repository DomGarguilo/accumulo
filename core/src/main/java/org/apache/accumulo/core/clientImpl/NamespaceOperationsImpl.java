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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.Validators.EXISTING_NAMESPACE_NAME;
import static org.apache.accumulo.core.util.Validators.NEW_NAMESPACE_NAME;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.manager.thrift.TFateOperation;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceOperationsImpl extends NamespaceOperationsHelper {
  private final ClientContext context;
  private final TableOperationsImpl tableOps;

  private static final Logger log = LoggerFactory.getLogger(TableOperations.class);

  public NamespaceOperationsImpl(ClientContext context, TableOperationsImpl tableOps) {
    checkArgument(context != null, "context is null");
    this.context = context;
    this.tableOps = tableOps;
  }

  @Override
  public SortedSet<String> list() {

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Fetching list of namespaces...", Thread.currentThread().getId());
      timer = Timer.startNew();
    }

    var namespaces = new TreeSet<>(context.getNamespaceMapping().getIdToNameMap().values());

    if (timer != null) {
      log.trace("tid={} Fetched {} namespaces in {}", Thread.currentThread().getId(),
          namespaces.size(), String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

    return namespaces;
  }

  @Override
  public boolean exists(String namespace) {
    EXISTING_NAMESPACE_NAME.validate(namespace);

    Timer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Checking if namespace {} exists", Thread.currentThread().getId(),
          namespace);
      timer = Timer.startNew();
    }

    boolean exists = false;
    try {
      context.getNamespaceId(namespace);
      exists = true;
    } catch (NamespaceNotFoundException e) {
      /* ignore */
    }

    if (timer != null) {
      log.trace("tid={} Checked existence of {} in {}", Thread.currentThread().getId(), exists,
          String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0));
    }

    return exists;
  }

  @Override
  public void create(String namespace)
      throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    NEW_NAMESPACE_NAME.validate(namespace);

    try {
      doNamespaceFateOperation(TFateOperation.NAMESPACE_CREATE,
          Arrays.asList(ByteBuffer.wrap(namespace.getBytes(UTF_8))), Collections.emptyMap(),
          namespace);
    } catch (NamespaceNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException, NamespaceNotEmptyException {
    EXISTING_NAMESPACE_NAME.validate(namespace);

    NamespaceId namespaceId = context.getNamespaceId(namespace);
    if (namespaceId.equals(Namespace.ACCUMULO.id()) || namespaceId.equals(Namespace.DEFAULT.id())) {
      Credentials credentials = context.getCredentials();
      log.debug("{} attempted to delete the {} namespace", credentials.getPrincipal(), namespaceId);
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.UNSUPPORTED_OPERATION);
    }

    if (!context.getTableMapping(namespaceId).getIdToNameMap().isEmpty()) {
      throw new NamespaceNotEmptyException(namespaceId.canonical(), namespace, null);
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doNamespaceFateOperation(TFateOperation.NAMESPACE_DELETE, args, opts, namespace);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName)
      throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {
    EXISTING_NAMESPACE_NAME.validate(oldNamespaceName);
    NEW_NAMESPACE_NAME.validate(newNamespaceName);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes(UTF_8)),
        ByteBuffer.wrap(newNamespaceName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();
    doNamespaceFateOperation(TFateOperation.NAMESPACE_RENAME, args, opts, oldNamespaceName);
  }

  @Override
  public void setProperty(final String namespace, final String property, final String value)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");

    try {
      ThriftClientTypes.MANAGER.executeVoidTableCommand(context,
          client -> client.setNamespaceProperty(TraceUtil.traceInfo(), context.rpcCreds(),
              namespace, property, value));
    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException) {
        throw (NamespaceNotFoundException) e.getCause();
      } else {
        throw new AccumuloException(e);
      }
    }

    checkLocalityGroups(namespace, property);
  }

  private Map<String,String> tryToModifyProperties(final String namespace,
      final Consumer<Map<String,String>> mapMutator)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {

    final TVersionedProperties vProperties =
        ThriftClientTypes.CLIENT.execute(context, client -> client
            .getVersionedNamespaceProperties(TraceUtil.traceInfo(), context.rpcCreds(), namespace));
    mapMutator.accept(vProperties.getProperties());

    // A reference to the map was passed to the user, maybe they still have the reference and are
    // modifying it. Buggy Accumulo code could attempt to make modifications to the map after this
    // point. Because of these potential issues, create an immutable snapshot of the map so that
    // from here on the code is assured to always be dealing with the same map.
    vProperties.setProperties(Map.copyOf(vProperties.getProperties()));

    try {
      // Send to server
      ThriftClientTypes.MANAGER.executeVoidTableCommand(context,
          client -> client.modifyNamespaceProperties(TraceUtil.traceInfo(), context.rpcCreds(),
              namespace, vProperties));

      for (String property : vProperties.getProperties().keySet()) {
        checkLocalityGroups(namespace, property);
      }

    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException) {
        throw (NamespaceNotFoundException) e.getCause();
      } else {
        throw new AccumuloException(e);
      }
    }

    return vProperties.getProperties();
  }

  @Override
  public Map<String,String> modifyProperties(final String namespace,
      final Consumer<Map<String,String>> mapMutator)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(mapMutator != null, "mapMutator is null");

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
        .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(30)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (true) {
      try {
        var props = tryToModifyProperties(namespace, mapMutator);
        retry.logCompletion(log, "Modifying properties for namespace " + namespace);
        return props;
      } catch (ConcurrentModificationException cme) {
        try {
          retry.logRetry(log, "Unable to modify namespace properties for " + namespace
              + " because of concurrent modification");
          retry.waitForNextAttempt(log, "Modify namespace properties for " + namespace);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } finally {
        retry.useRetry();
      }
    }
  }

  @Override
  public void removeProperty(final String namespace, final String property)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(property != null, "property is null");
    try {
      ThriftClientTypes.MANAGER.executeVoidTableCommand(context, client -> client
          .removeNamespaceProperty(TraceUtil.traceInfo(), context.rpcCreds(), namespace, property));
    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException) {
        throw (NamespaceNotFoundException) e.getCause();
      } else {
        throw new AccumuloException(e);
      }
    }
    checkLocalityGroups(namespace, property);
  }

  @Override
  public Map<String,String> getConfiguration(final String namespace)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);

    try {
      return ThriftClientTypes.CLIENT.execute(context, client -> client
          .getNamespaceConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), namespace));
    } catch (AccumuloSecurityException e) {
      throw e;
    } catch (AccumuloException e) {
      Throwable eCause = e.getCause();
      if (eCause instanceof TableNotFoundException) {
        Throwable tnfeCause = eCause.getCause();
        if (tnfeCause instanceof NamespaceNotFoundException) {
          var nnfe = (NamespaceNotFoundException) tnfeCause;
          nnfe.addSuppressed(e);
          throw nnfe;
        }
      }
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public Map<String,String> getNamespaceProperties(String namespace)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);

    try {
      return ThriftClientTypes.CLIENT.execute(context, client -> client
          .getNamespaceProperties(TraceUtil.traceInfo(), context.rpcCreds(), namespace));
    } catch (AccumuloException e) {
      Throwable eCause = e.getCause();
      if (eCause instanceof TableNotFoundException) {
        Throwable tnfeCause = eCause.getCause();
        if (tnfeCause instanceof NamespaceNotFoundException) {
          var nnfe = (NamespaceNotFoundException) tnfeCause;
          nnfe.addSuppressed(e);
          throw nnfe;
        }
      }
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public Map<String,String> namespaceIdMap() {
    var result = new TreeMap<String,String>();
    context.getNamespaceMapping().getIdToNameMap().forEach(
        (namespaceId, namespaceName) -> result.put(namespaceName, namespaceId.canonical()));
    return result;
  }

  @Override
  public boolean testClassLoad(final String namespace, final String className,
      final String asTypeName)
      throws NamespaceNotFoundException, AccumuloException, AccumuloSecurityException {
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(className != null, "className is null");
    checkArgument(asTypeName != null, "asTypeName is null");

    try {
      return ThriftClientTypes.CLIENT.execute(context,
          client -> client.checkNamespaceClass(TraceUtil.traceInfo(), context.rpcCreds(), namespace,
              className, asTypeName));
    } catch (AccumuloSecurityException e) {
      throw e;
    } catch (AccumuloException e) {
      Throwable eCause = e.getCause();
      if (eCause instanceof TableNotFoundException) {
        Throwable tnfeCause = eCause.getCause();
        if (tnfeCause instanceof NamespaceNotFoundException) {
          var nnfe = (NamespaceNotFoundException) tnfeCause;
          nnfe.addSuppressed(e);
          throw nnfe;
        }
      }
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }

  }

  @Override
  public void attachIterator(String namespace, IteratorSetting setting,
      EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException {
    // testClassLoad validates the namespace name
    testClassLoad(namespace, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(namespace, setting, scopes);
  }

  @Override
  public int addConstraint(String namespace, String constraintClassName)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    // testClassLoad validates the namespace name
    testClassLoad(namespace, constraintClassName, Constraint.class.getName());
    return super.addConstraint(namespace, constraintClassName);
  }

  private String doNamespaceFateOperation(TFateOperation op, List<ByteBuffer> args,
      Map<String,String> opts, String namespace) throws AccumuloSecurityException,
      AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    // caller should validate the namespace name
    try {
      return tableOps.doFateOperation(op, args, opts, namespace);
    } catch (TableExistsException | TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private void checkLocalityGroups(String namespace, String propChanged)
      throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException {
    EXISTING_NAMESPACE_NAME.validate(namespace);

    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      Map<String,String> allProps = getConfiguration(namespace);
      try {
        LocalityGroupUtil.checkLocalityGroups(allProps);
      } catch (LocalityGroupConfigurationError | RuntimeException e) {
        LoggerFactory.getLogger(this.getClass()).warn("Changing '" + propChanged
            + "' for namespace '" + namespace
            + "'resulted in bad locality group config. This may be a transient situation since the"
            + " config spreads over multiple properties. Setting properties in a different order "
            + "may help. Even though this warning was displayed, the property was updated. Please "
            + "check your config to ensure consistency.", e);
      }
    }
  }
}
