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
package org.apache.accumulo.core.metadata.schema.filters;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TabletMetadataFilter extends RowFilter {

  private static final Logger LOG = LoggerFactory.getLogger(TabletMetadataFilter.class);

  @Override
  public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) {
    TabletMetadata tm = TabletMetadata.convertRow(new IteratorAdapter(rowIterator),
        EnumSet.copyOf(getColumns()), true, false);
    Predicate<TabletMetadata> predicate = acceptTablet();
    if (predicate == null) {
      return false;
    }
    boolean result = predicate.test(tm);
    LOG.trace("Filter {} returned {} for tablet metadata: {}", this.getClass().getSimpleName(),
        result, tm.getKeyValues());
    return result;
  }

  public abstract Set<TabletMetadata.ColumnType> getColumns();

  protected abstract Predicate<TabletMetadata> acceptTablet();

  public Map<String,String> getServerSideOptions() {
    return Map.of();
  }
}
