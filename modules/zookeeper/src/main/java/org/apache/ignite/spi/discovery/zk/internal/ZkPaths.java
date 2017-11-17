/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.UUID;

/**
 *
 */
class ZkPaths {
    /** */
    static final int ID_LEN = 36;

    /** */
    private static final String JOIN_DATA_DIR = "joinData";

    /** */
    private static final String JOIN_EVENTS_DATA_PATH = "joinEvents";

    /** */
    private static final String ALIVE_NODES_DIR = "alive";

    /** */
    private static final String DISCO_EVENTS_PATH = "events";

    /** */
    final String basePath;

    /** */
    private final String clusterName;

    /** */
    final String aliveNodesDir;

    /** */
    final String joinEvtsDataDir;

    /** */
    final String joinDataDir;

    /** */
    final String evtsPath;

    /**
     * @param basePath Base directory.
     * @param clusterName Cluster name.
     */
    ZkPaths(String basePath, String clusterName) {
        this.basePath = basePath;
        this.clusterName = clusterName;

        aliveNodesDir = zkPath(ALIVE_NODES_DIR);
        joinDataDir = zkPath(JOIN_DATA_DIR);
        joinEvtsDataDir = zkPath(JOIN_EVENTS_DATA_PATH);
        evtsPath = zkPath(DISCO_EVENTS_PATH);
    }

    /**
     * @param path Relative path.
     * @return Full path.
     */
    String zkPath(String path) {
        return basePath + "/" + clusterName + "/" + path;
    }

    static int aliveInternalId(String path) {
        int idx = path.lastIndexOf('|');

        return Integer.parseInt(path.substring(idx + 1));
    }

    static UUID aliveNodeId(String path) {
        String idStr = path.substring(0, ZkPaths.ID_LEN);

        return UUID.fromString(idStr);
    }

    static int aliveJoinSequence(String path) {
        int idx1 = path.indexOf('|');
        int idx2 = path.lastIndexOf('|');

        return Integer.parseInt(path.substring(idx1 + 1, idx2));
    }
}
