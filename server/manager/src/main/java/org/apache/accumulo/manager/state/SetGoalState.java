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
package org.apache.accumulo.manager.state;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SecurityUtil;

import com.google.common.base.Preconditions;

public class SetGoalState {

  /**
   * Utility program that will change the goal state for the manager from the command line.
   */
  public static void main(String[] args) throws Exception {
    try {
      Preconditions.checkArgument(args.length == 1);
      ManagerGoalState.valueOf(args[0]);
    } catch (IllegalArgumentException e) {
      System.err.println(
          "Usage: accumulo " + SetGoalState.class.getName() + " [NORMAL|SAFE_MODE|CLEAN_STOP]");
      System.exit(-1);
    }

    var siteConfig = SiteConfiguration.auto();
    SecurityUtil.serverLogin(siteConfig);
    try (var context = new ServerContext(siteConfig)) {
      context.waitForZookeeperAndHdfs();
      context.getZooSession().asReaderWriter().putPersistentData(Constants.ZMANAGER_GOAL_STATE,
          args[0].getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    }
  }

}
