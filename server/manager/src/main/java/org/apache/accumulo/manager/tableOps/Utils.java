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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.DistributedLock;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.FateLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooReservation;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final byte[] ZERO_BYTE = {'0'};
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  public static <T extends AbstractId<T>> T getNextId(String name, ServerContext context,
      Function<String,T> newIdFunction) throws AcceptableThriftTableOperationException {
    try {
      ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
      byte[] nid = zoo.mutateOrCreate(Constants.ZTABLES, ZERO_BYTE, currentValue -> {
        BigInteger nextId = new BigInteger(new String(currentValue, UTF_8), Character.MAX_RADIX);
        nextId = nextId.add(BigInteger.ONE);
        return nextId.toString(Character.MAX_RADIX).getBytes(UTF_8);
      });
      return newIdFunction.apply(new String(nid, UTF_8));
    } catch (Exception e1) {
      log.error("Failed to assign id to " + name, e1);
      throw new AcceptableThriftTableOperationException(null, name, TableOperation.CREATE,
          TableOperationExceptionType.OTHER, e1.getMessage());
    }
  }

  static final Lock tableNameLock = new ReentrantLock();
  static final Lock idLock = new ReentrantLock();

  public static long reserveTable(Manager env, TableId tableId, FateId fateId, LockType lockType,
      boolean tableMustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), tableId, fateId, lockType).tryLock()) {
      if (tableMustExist) {
        ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();
        if (!zk.exists(Constants.ZTABLES + "/" + tableId)) {
          throw new AcceptableThriftTableOperationException(tableId.canonical(), "", op,
              TableOperationExceptionType.NOTFOUND, "Table does not exist");
        }
      }
      log.info("table {} {} locked for {} operation: {}", tableId, fateId, lockType, op);
      return 0;
    } else {
      return 100;
    }
  }

  public static void unreserveTable(Manager env, TableId tableId, FateId fateId,
      LockType lockType) {
    getLock(env.getContext(), tableId, fateId, lockType).unlock();
    log.info("table {} {} unlocked for {}", tableId, fateId, lockType);
  }

  public static void unreserveNamespace(Manager env, NamespaceId namespaceId, FateId fateId,
      LockType lockType) {
    getLock(env.getContext(), namespaceId, fateId, lockType).unlock();
    log.info("namespace {} {} unlocked for {}", namespaceId, fateId, lockType);
  }

  public static long reserveNamespace(Manager env, NamespaceId namespaceId, FateId fateId,
      LockType lockType, boolean mustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), namespaceId, fateId, lockType).tryLock()) {
      if (mustExist) {
        ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();
        if (!zk.exists(Constants.ZNAMESPACES + "/" + namespaceId)) {
          throw new AcceptableThriftTableOperationException(namespaceId.canonical(), "", op,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "Namespace does not exist");
        }
      }
      log.info("namespace {} {} locked for {} operation: {}", namespaceId, fateId, lockType, op);
      return 0;
    } else {
      return 100;
    }
  }

  public static long reserveHdfsDirectory(Manager env, String directory, FateId fateId)
      throws KeeperException, InterruptedException {

    ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();

    if (ZooReservation.attempt(zk, Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8)), fateId, "")) {
      return 0;
    } else {
      return 50;
    }
  }

  public static void unreserveHdfsDirectory(Manager env, String directory, FateId fateId)
      throws KeeperException, InterruptedException {
    ZooReservation.release(env.getContext().getZooSession().asReaderWriter(),
        Constants.ZHDFS_RESERVATIONS + "/"
            + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8)),
        fateId);
  }

  private static Lock getLock(ServerContext context, AbstractId<?> id, FateId fateId,
      LockType lockType) {
    FateLock qlock = new FateLock(context.getZooSession().asReaderWriter(),
        FateLock.path(Constants.ZTABLE_LOCKS + "/" + id.canonical()));
    DistributedLock lock = DistributedReadWriteLock.recoverLock(qlock, fateId);
    if (lock != null) {

      // Validate the recovered lock type
      if (lock.getType() != lockType) {
        throw new IllegalStateException(
            "Unexpected lock type " + lock.getType() + " recovered for transaction " + fateId
                + " on object " + id + ". Expected " + lockType + " lock instead.");
      }
    } else {
      DistributedReadWriteLock locker = new DistributedReadWriteLock(qlock, fateId);
      switch (lockType) {
        case WRITE:
          lock = locker.writeLock();
          break;
        case READ:
          lock = locker.readLock();
          break;
        default:
          throw new IllegalStateException("Unexpected LockType: " + lockType);
      }
    }
    return lock;
  }

  public static Lock getIdLock() {
    return idLock;
  }

  public static Lock getTableNameLock() {
    return tableNameLock;
  }

  public static Lock getReadLock(Manager env, AbstractId<?> id, FateId fateId) {
    return Utils.getLock(env.getContext(), id, fateId, LockType.READ);
  }

  /**
   * Given a fully-qualified Path and a flag indicating if the file info is base64 encoded or not,
   * retrieve the data from a file on the file system. It is assumed that the file is textual and
   * not binary data.
   *
   * @param path the fully-qualified path
   */
  public static SortedSet<Text> getSortedSetFromFile(Manager manager, Path path, boolean encoded)
      throws IOException {
    FileSystem fs = path.getFileSystem(manager.getContext().getHadoopConf());
    var data = new TreeSet<Text>();
    try (var file = new java.util.Scanner(fs.open(path), UTF_8)) {
      while (file.hasNextLine()) {
        String line = file.nextLine();
        data.add(encoded ? new Text(Base64.getDecoder().decode(line)) : new Text(line));
      }
    }
    return data;
  }

  public static SortedMap<Text,TabletMergeability> getSortedSplitsFromFile(Manager manager,
      Path path) throws IOException {
    FileSystem fs = path.getFileSystem(manager.getContext().getHadoopConf());
    var data = new TreeMap<Text,TabletMergeability>();
    try (var file = new java.util.Scanner(fs.open(path), UTF_8)) {
      while (file.hasNextLine()) {
        String line = file.nextLine();
        log.trace("split line: {}", line);
        Pair<Text,TabletMergeability> splitTm = TabletMergeabilityUtil.decode(line);
        data.put(splitTm.getFirst(), splitTm.getSecond());
      }
    }
    return data;
  }

}
