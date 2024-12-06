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
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.metrics.thrift;


public enum MetricSource implements org.apache.thrift.TEnum {
  COMPACTOR(0),
  GARBAGE_COLLECTOR(1),
  MANAGER(2),
  SCAN_SERVER(3),
  TABLET_SERVER(4);

  private final int value;

  private MetricSource(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  @Override
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static MetricSource findByValue(int value) { 
    switch (value) {
      case 0:
        return COMPACTOR;
      case 1:
        return GARBAGE_COLLECTOR;
      case 2:
        return MANAGER;
      case 3:
        return SCAN_SERVER;
      case 4:
        return TABLET_SERVER;
      default:
        return null;
    }
  }
}