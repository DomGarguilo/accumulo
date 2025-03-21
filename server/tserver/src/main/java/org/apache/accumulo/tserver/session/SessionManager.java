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
package org.apache.accumulo.tserver.session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.tabletscan.thrift.ActiveScan;
import org.apache.accumulo.core.tabletscan.thrift.ScanState;
import org.apache.accumulo.core.tabletscan.thrift.ScanType;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.scan.ScanRunState;
import org.apache.accumulo.tserver.scan.ScanTask;
import org.apache.accumulo.tserver.session.Session.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SessionManager {
  private static final Logger log = LoggerFactory.getLogger(SessionManager.class);

  private final ConcurrentMap<Long,Session> sessions = new ConcurrentHashMap<>();
  private final BlockingQueue<Session> deferredCleanupQueue = new ArrayBlockingQueue<>(5000);
  private final ServerContext ctx;
  private volatile LongConsumer zombieCountConsumer = null;

  public SessionManager(ServerContext context) {
    this.ctx = context;
    long maxIdle = ctx.getConfiguration().getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);
    Runnable r = () -> sweep(ctx);
    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor().scheduleWithFixedDelay(r,
        0, Math.max(maxIdle / 2, 1000), MILLISECONDS));
  }

  public long createSession(Session session, boolean reserve) {
    long sid = RANDOM.get().nextLong();

    synchronized (session) {
      Preconditions.checkArgument(session.getState() == State.NEW);
      session.setState(reserve ? State.RESERVED : State.UNRESERVED);
      session.startTime = session.lastAccessTime = System.currentTimeMillis();
      while (sessions.putIfAbsent(sid, session) != null) {
        sid = RANDOM.get().nextLong();
      }
      session.setSessionId(sid);
    }

    return sid;
  }

  public long getMaxIdleTime() {
    return ctx.getConfiguration().getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);
  }

  /**
   * while a session is reserved, it cannot be canceled or removed
   */

  public Session reserveSession(long sessionId) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      synchronized (session) {
        if (session.getState() == State.RESERVED) {
          throw new IllegalStateException(
              "Attempted to reserve session that is already reserved " + sessionId);
        }
        if (session.getState() == State.REMOVED || !session.allowReservation) {
          return null;
        }
        session.setState(State.RESERVED);
      }
    }

    return session;

  }

  public Session reserveSession(long sessionId, boolean wait) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      synchronized (session) {

        if (session.getState() == State.REMOVED || !session.allowReservation) {
          return null;
        }

        while (wait && session.getState() == State.RESERVED) {
          try {
            session.wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException();
          }
        }

        if (session.getState() == State.RESERVED) {
          throw new IllegalStateException(
              "Attempted to reserved session that is already reserved " + sessionId);
        }
        if (session.getState() == State.REMOVED || !session.allowReservation) {
          return null;
        }
        session.setState(State.RESERVED);
      }
    }

    return session;

  }

  public void unreserveSession(Session session) {
    synchronized (session) {
      if (session.getState() == State.REMOVED) {
        return;
      }
      if (session.getState() != State.RESERVED) {
        throw new IllegalStateException("Cannon unreserve, state: " + session.getState());
      }

      session.notifyAll();
      session.setState(State.UNRESERVED);
      session.lastAccessTime = System.currentTimeMillis();
    }
  }

  public void unreserveSession(long sessionId) {
    Session session = getSession(sessionId);
    if (session != null) {
      unreserveSession(session);
    }
  }

  public Session getSession(long sessionId) {
    Session session = sessions.get(sessionId);

    if (session != null) {
      synchronized (session) {
        if (session.getState() == State.REMOVED) {
          return null;
        }
        session.lastAccessTime = System.currentTimeMillis();
      }
    }

    return session;
  }

  public Session removeSession(long sessionId) {
    return removeSession(sessionId, false);
  }

  public Session removeSession(long sessionId, boolean unreserve) {

    Session session = sessions.remove(sessionId);
    if (session != null) {
      boolean doCleanup = false;
      synchronized (session) {
        if (session.getState() != State.REMOVED) {
          if (unreserve) {
            unreserveSession(session);
          }
          doCleanup = true;
          session.setState(State.REMOVED);
        }
      }

      if (doCleanup) {
        cleanup(session);
      }
    }

    return session;
  }

  public boolean removeIfNotReserved(long sessionId) {
    Session session = sessions.get(sessionId);
    if (session == null) {
      // it was already removed
      return true;
    }

    synchronized (session) {
      if (session.getState() == State.RESERVED) {
        return false;
      }
      session.setState(State.REMOVED);
    }

    sessions.remove(sessionId);

    return true;
  }

  /**
   * Prevents a session from ever being reserved in the future. This method can be called
   * concurrently when another thread has the session reserved w/o impacting the other thread. When
   * the session is currently reserved by another thread that thread can unreserve as normal and
   * after that this session can never be reserved again. Since the session can never be reserved
   * after this call it will eventually age off and be cleaned up.
   *
   * @return true if the sessions is currently not reserved, false otherwise
   */
  public boolean disallowNewReservations(long sessionId) {
    var session = getSession(sessionId);
    if (session == null) {
      return true;
    }
    synchronized (session) {
      if (session.allowReservation) {
        // Prevent future reservations of this session.
        session.allowReservation = false;
        log.debug("disabled session {}", sessionId);
      }

      // If nothing can reserve the session and it is not currently reserved then the session is
      // disabled and will eventually be cleaned up.
      return session.getState() != State.RESERVED;
    }
  }

  static void cleanup(BlockingQueue<Session> deferredCleanupQueue, Session session) {
    if (!session.cleanup()) {
      var retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
          .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(5)).backOffFactor(1.5)
          .logInterval(Duration.ofMinutes(1)).createRetry();

      while (!deferredCleanupQueue.offer(session)) {
        if (session.cleanup()) {
          break;
        }

        try {
          retry.waitForNextAttempt(log, "Unable to cleanup session or defer cleanup " + session);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        retry.logRetry(log, "Unable to cleanup session or defer cleanup " + session);
      }

      retry.logCompletion(log, "Cleaned up session or deferred cleanup " + session);
    }
  }

  private void cleanup(Session session) {
    cleanup(deferredCleanupQueue, session);
  }

  private void sweep(final ServerContext context) {
    final AccumuloConfiguration conf = context.getConfiguration();
    final long maxUpdateIdle = conf.getTimeInMillis(Property.TSERV_UPDATE_SESSION_MAXIDLE);
    final long maxIdle = conf.getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);
    List<Session> sessionsToCleanup = new LinkedList<>();
    Iterator<Session> iter = sessions.values().iterator();
    while (iter.hasNext()) {
      Session session = iter.next();
      synchronized (session) {
        if (session.getState() == State.UNRESERVED) {
          long configuredIdle = maxIdle;
          if (session instanceof UpdateSession) {
            configuredIdle = maxUpdateIdle;
          }
          long idleTime = System.currentTimeMillis() - session.lastAccessTime;
          if (idleTime > configuredIdle) {
            log.trace("Closing idle session {} from user={}, client={}, idle={}ms",
                session.getSessionId(), session.getUser(), session.client, idleTime);
            iter.remove();
            sessionsToCleanup.add(session);
            session.setState(State.REMOVED);
          }
        }
      }
    }

    // do clean up outside of lock for TabletServer
    deferredCleanupQueue.drainTo(sessionsToCleanup);

    // make a pass through and remove everything that can be cleaned up before calling the
    // cleanup(Session) method which may block when it can not clean up a session.
    sessionsToCleanup.removeIf(Session::cleanup);

    sessionsToCleanup.forEach(this::cleanup);

    if (zombieCountConsumer != null) {
      zombieCountConsumer.accept(countZombieScans(maxIdle));
    }
  }

  private long countZombieScans(long reportTimeMillis) {
    return Stream.concat(deferredCleanupQueue.stream(), sessions.values().stream())
        .filter(session -> {
          if (session instanceof ScanSession) {
            var scanSession = (ScanSession<?>) session;
            synchronized (scanSession) {
              var scanTask = scanSession.getScanTask();
              if (scanTask != null && scanSession.getState() == State.REMOVED
                  && scanTask.getScanThread() != null
                  && scanSession.elaspedSinceStateChange(MILLISECONDS) > reportTimeMillis) {
                scanSession.logZombieStackTrace();
                return true;
              }
            }
          }
          return false;
        }).count();
  }

  public void setZombieCountConsumer(LongConsumer zombieCountConsumer) {
    this.zombieCountConsumer = Objects.requireNonNull(zombieCountConsumer);
  }

  public void removeIfNotAccessed(final long sessionId, final long delay) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      long tmp;
      synchronized (session) {
        tmp = session.lastAccessTime;
      }
      final long removeTime = tmp;
      Runnable r = new Runnable() {
        @Override
        public void run() {
          Session session2 = sessions.get(sessionId);
          if (session2 != null) {
            boolean shouldRemove = false;
            synchronized (session2) {
              if (session2.lastAccessTime == removeTime
                  && session2.getState() == State.UNRESERVED) {
                session2.setState(State.REMOVED);
                shouldRemove = true;
              }
            }

            if (shouldRemove) {
              log.info("Closing not accessed session " + session2.getSessionId() + " from user="
                  + session2.getUser() + ", client=" + session2.client + ", duration=" + delay
                  + "ms");
              sessions.remove(sessionId);
              cleanup(session2);
            }
          }
        }
      };

      ScheduledFuture<?> future = ctx.getScheduledExecutor().schedule(r, delay, MILLISECONDS);
      ThreadPools.watchNonCriticalScheduledTask(future);
    }
  }

  public Map<TableId,MapCounter<ScanRunState>> getActiveScansPerTable() {
    Map<TableId,MapCounter<ScanRunState>> counts = new HashMap<>();

    Stream.concat(sessions.values().stream(), deferredCleanupQueue.stream()).forEach(session -> {
      ScanTask<?> nbt = null;
      TableId tableID = null;

      if (session instanceof SingleScanSession) {
        SingleScanSession ss = (SingleScanSession) session;
        nbt = ss.getScanTask();
        tableID = ss.extent.tableId();
      } else if (session instanceof MultiScanSession) {
        MultiScanSession mss = (MultiScanSession) session;
        nbt = mss.getScanTask();
        tableID = mss.threadPoolExtent.tableId();
      }

      if (nbt != null) {
        ScanRunState srs = nbt.getScanRunState();
        if (srs != ScanRunState.FINISHED) {
          counts.computeIfAbsent(tableID, unusedKey -> new MapCounter<>()).increment(srs, 1);
        }
      }
    });

    return counts;
  }

  public List<ActiveScan> getActiveScans() {

    final List<ActiveScan> activeScans = new ArrayList<>();
    final long ct = System.currentTimeMillis();

    Stream.concat(sessions.values().stream(), deferredCleanupQueue.stream()).forEach(session -> {
      if (session instanceof ScanSession) {
        ScanSession<?> scanSession = (ScanSession<?>) session;
        boolean isSingle = session instanceof SingleScanSession;

        addActiveScan(activeScans, scanSession,
            isSingle ? ((SingleScanSession) scanSession).extent
                : ((MultiScanSession) scanSession).threadPoolExtent,
            ct, isSingle ? ScanType.SINGLE : ScanType.BATCH,
            computeScanState(scanSession.getScanTask()), scanSession.scanParams,
            session.getSessionId());
      }
    });

    return activeScans;
  }

  private ScanState computeScanState(ScanTask<?> scanTask) {
    ScanState state = ScanState.RUNNING;

    if (scanTask == null) {
      state = ScanState.IDLE;
    } else {
      switch (scanTask.getScanRunState()) {
        case QUEUED:
          state = ScanState.QUEUED;
          break;
        case FINISHED:
          state = ScanState.IDLE;
          break;
        case RUNNING:
        default:
          /* do nothing */
          break;
      }
    }

    return state;
  }

  private void addActiveScan(List<ActiveScan> activeScans, Session session, KeyExtent extent,
      long ct, ScanType scanType, ScanState state, ScanParameters params, long scanId) {
    ActiveScan activeScan =
        new ActiveScan(session.client, session.getUser(), extent.tableId().canonical(),
            ct - session.startTime, ct - session.lastAccessTime, scanType, state, extent.toThrift(),
            params.getColumnSet().stream().map(Column::toThrift).collect(Collectors.toList()),
            params.getSsiList(), params.getSsio(), params.getAuthorizations().getAuthorizationsBB(),
            params.getClassLoaderContext());
    // scanId added by ACCUMULO-2641 is an optional thrift argument and not available in
    // ActiveScan constructor
    activeScan.setScanId(scanId);
    activeScans.add(activeScan);
  }
}
