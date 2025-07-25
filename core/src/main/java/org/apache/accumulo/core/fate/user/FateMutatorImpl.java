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
package org.apache.accumulo.core.fate.user;

import static org.apache.accumulo.core.fate.AbstractFateStore.serialize;
import static org.apache.accumulo.core.fate.user.UserFateStore.getRow;
import static org.apache.accumulo.core.fate.user.UserFateStore.getRowId;
import static org.apache.accumulo.core.fate.user.UserFateStore.invertRepo;

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.user.schema.FateSchema.RepoColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxAdminColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxInfoColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class FateMutatorImpl<T> implements FateMutator<T> {

  private final ClientContext context;
  private final String tableName;
  private final FateId fateId;
  private final ConditionalMutation mutation;
  private final Supplier<ConditionalWriter> writer;
  private boolean requiredUnreserved = false;
  public static final int INITIAL_ITERATOR_PRIO = 1000000;

  public FateMutatorImpl(ClientContext context, String tableName, FateId fateId,
      Supplier<ConditionalWriter> writer) {
    this.context = Objects.requireNonNull(context);
    this.tableName = Objects.requireNonNull(tableName);
    this.fateId = Objects.requireNonNull(fateId);
    this.mutation = new ConditionalMutation(new Text(getRowId(fateId)));
    this.writer = Objects.requireNonNull(writer);
  }

  @Override
  public FateMutator<T> putStatus(TStatus status) {
    TxAdminColumnFamily.STATUS_COLUMN.put(mutation, new Value(status.name()));
    return this;
  }

  @Override
  public FateMutator<T> putKey(FateKey fateKey) {
    TxColumnFamily.TX_KEY_COLUMN.put(mutation, new Value(fateKey.getSerialized()));
    return this;
  }

  @Override
  public FateMutator<T> putCreateTime(long ctime) {
    TxColumnFamily.CREATE_TIME_COLUMN.put(mutation, new Value(Long.toString(ctime)));
    return this;
  }

  @Override
  public FateMutator<T> requireAbsent() {
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, RowExistsIterator.class);
    Condition c = new Condition("", "").setIterators(is);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public FateMutator<T> requireUnreserved() {
    Preconditions.checkState(!requiredUnreserved);
    Condition condition = new Condition(TxAdminColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
        TxAdminColumnFamily.RESERVATION_COLUMN.getColumnQualifier());
    mutation.addCondition(condition);
    requiredUnreserved = true;
    return this;
  }

  @Override
  public FateMutator<T> requireAbsentKey() {
    Condition condition = new Condition(TxColumnFamily.TX_KEY_COLUMN.getColumnFamily(),
        TxColumnFamily.TX_KEY_COLUMN.getColumnQualifier());
    mutation.addCondition(condition);
    return this;
  }

  @Override
  public FateMutator<T> putReservedTx(FateStore.FateReservation reservation) {
    requireUnreserved();
    TxAdminColumnFamily.RESERVATION_COLUMN.put(mutation, new Value(reservation.getSerialized()));
    return this;
  }

  @Override
  public FateMutator<T> putUnreserveTx(FateStore.FateReservation reservation) {
    Condition condition = new Condition(TxAdminColumnFamily.RESERVATION_COLUMN.getColumnFamily(),
        TxAdminColumnFamily.RESERVATION_COLUMN.getColumnQualifier())
        .setValue(reservation.getSerialized());
    mutation.addCondition(condition);
    TxAdminColumnFamily.RESERVATION_COLUMN.putDelete(mutation);
    return this;
  }

  @Override
  public FateMutator<T> putFateOp(byte[] data) {
    TxAdminColumnFamily.FATE_OP_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putAutoClean(byte[] data) {
    TxInfoColumnFamily.AUTO_CLEAN_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putException(byte[] data) {
    TxInfoColumnFamily.EXCEPTION_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putReturnValue(byte[] data) {
    TxInfoColumnFamily.RETURN_VALUE_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putAgeOff(byte[] data) {
    TxInfoColumnFamily.TX_AGEOFF_COLUMN.put(mutation, new Value(data));
    return this;
  }

  @Override
  public FateMutator<T> putTxInfo(TxInfo txInfo, byte[] data) {
    switch (txInfo) {
      case FATE_OP:
        putFateOp(data);
        break;
      case AUTO_CLEAN:
        putAutoClean(data);
        break;
      case EXCEPTION:
        putException(data);
        break;
      case RETURN_VALUE:
        putReturnValue(data);
        break;
      case TX_AGEOFF:
        putAgeOff(data);
        break;
      default:
        throw new IllegalArgumentException("Unexpected TxInfo type: " + txInfo);
    }
    return this;
  }

  @Override
  public FateMutator<T> putRepo(int position, Repo<T> repo) {
    final Text cq = invertRepo(position);
    // ensure this repo is not already set
    mutation.addCondition(new Condition(RepoColumnFamily.NAME, cq));
    mutation.put(RepoColumnFamily.NAME, cq, new Value(serialize(repo)));
    return this;
  }

  @Override
  public FateMutator<T> deleteRepo(int position) {
    mutation.putDelete(RepoColumnFamily.NAME, invertRepo(position));
    return this;
  }

  public FateMutator<T> delete() {
    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(getRow(fateId));
      scanner.forEach(
          (key, value) -> mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier()));
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(tableName + " not found!", e);
    }
    return this;
  }

  @Override
  public FateMutator<T> requireStatus(TStatus... statuses) {
    Condition condition = StatusMappingIterator.createCondition(statuses);
    mutation.addCondition(condition);
    return this;
  }

  @Override
  public void mutate() {
    var status = tryMutate();
    if (status != Status.ACCEPTED) {
      throw new IllegalStateException("Failed to write mutation " + status + " " + mutation);
    }
  }

  @Override
  public Status tryMutate() {
    try {
      // if there are no conditions attached, then we can use a batch writer
      if (mutation.getConditions().isEmpty()) {
        try (BatchWriter writer = context.createBatchWriter(tableName)) {
          writer.addMutation(mutation);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }

        return Status.ACCEPTED;
      } else {
        try {
          ConditionalWriter.Result result = writer.get().write(mutation);

          switch (result.getStatus()) {
            case ACCEPTED:
              return Status.ACCEPTED;
            case REJECTED:
              return Status.REJECTED;
            case UNKNOWN:
              return Status.UNKNOWN;
            default:
              // do not expect other statuses
              throw new IllegalStateException(
                  "Unhandled status for mutation " + result.getStatus());
          }

        } catch (AccumuloException | AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ConditionalMutation getMutation() {
    return mutation;
  }
}
