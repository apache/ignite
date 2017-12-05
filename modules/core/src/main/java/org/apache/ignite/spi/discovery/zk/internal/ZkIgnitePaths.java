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
class ZkIgnitePaths {
    /** */
    private static final int UUID_LEN = 36;

    /** */
    private static final String JOIN_DATA_DIR = "jd";

    /** */
    private static final String CUSTOM_EVTS_DIR = "c";

    /** */
    private static final String CUSTOM_EVTS_ACKS_DIR = "ca";

    /** */
    private static final String ALIVE_NODES_DIR = "n";

    /** */
    private static final String DISCO_EVENTS_PATH = "e";

    /** */
    final String clusterDir;

    /** */
    final String aliveNodesDir;

    /** */
    final String joinDataDir;

    /** */
    final String evtsPath;

    /** */
    final String customEvtsDir;

    /** */
    final String customEvtsAcksDir;

    /**
     * @param zkRootPath Base Zookeeper directory for all Ignite nodes.
     */
    ZkIgnitePaths(String zkRootPath) {
        clusterDir = zkRootPath;

        aliveNodesDir = zkPath(ALIVE_NODES_DIR);
        joinDataDir = zkPath(JOIN_DATA_DIR);
        evtsPath = zkPath(DISCO_EVENTS_PATH);
        customEvtsDir = zkPath(CUSTOM_EVTS_DIR);
        customEvtsAcksDir = zkPath(CUSTOM_EVTS_ACKS_DIR);
    }

    /**
     * TODO ZK: copied from curator.
     *
     * Validate the provided znode path string
     * @param path znode path string
     * @return The given path if it was valid, for fluent chaining
     * @throws IllegalArgumentException if the path is invalid
     */
    static String validatePath(String path) throws IllegalArgumentException {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Path length must be > 0");
        }
        if (path.charAt(0) != '/') {
            throw new IllegalArgumentException(
                "Path must start with / character");
        }
        if (path.length() == 1) { // done checking - it's the root
            return path;
        }
        if (path.charAt(path.length() - 1) == '/') {
            throw new IllegalArgumentException(
                "Path must not end with / character");
        }

        String reason = null;
        char lastc = '/';
        char chars[] = path.toCharArray();
        char c;
        for (int i = 1; i < chars.length; lastc = chars[i], i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            } else if (c == '/' && lastc == '/') {
                reason = "empty node name specified @" + i;
                break;
            } else if (c == '.' && lastc == '.') {
                if (chars[i-2] == '/' &&
                    ((i + 1 == chars.length)
                        || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c == '.') {
                if (chars[i-1] == '/' &&
                    ((i + 1 == chars.length)
                        || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c > '\u0000' && c < '\u001f'
                || c > '\u007f' && c < '\u009F'
                || c > '\ud800' && c < '\uf8ff'
                || c > '\ufff0' && c < '\uffff') {
                reason = "invalid charater @" + i;
                break;
            }
        }

        if (reason != null) {
            throw new IllegalArgumentException(
                "Invalid path string \"" + path + "\" caused by " + reason);
        }

        return path;
    }

    /**
     * @param path Relative path.
     * @return Full path.
     */
    private String zkPath(String path) {
        return clusterDir + "/" + path;
    }

    String joiningNodeDataPath(UUID nodeId, UUID prefixId) {
        return joinDataDir + '/' + prefixId + ":" + nodeId.toString();
    }

    /**
     * @param path Alive node zk path.
     * @return Node internal ID.
     */
    static int aliveInternalId(String path) {
        int idx = path.lastIndexOf('|');

        return Integer.parseInt(path.substring(idx + 1));
    }

    /**
     * @param path Alive node zk path.
     * @return Node ID.
     */
    static UUID aliveNodePrefixId(String path) {
        return UUID.fromString(path.substring(0, ZkIgnitePaths.UUID_LEN));
    }

    /**
     * @param path Alive node zk path.
     * @return Node ID.
     */
    static UUID aliveNodeId(String path) {
        // <uuid prefix>:<node id>|<alive seq>
        int startIdx = ZkIgnitePaths.UUID_LEN + 1;

        String idStr = path.substring(startIdx, startIdx + ZkIgnitePaths.UUID_LEN);

        return UUID.fromString(idStr);
    }

    /**
     * @param path Event zk path.
     * @return Event sequence number.
     */
    static int customEventSequence(String path) {
        int idx = path.lastIndexOf('|');

        return Integer.parseInt(path.substring(idx + 1));
    }

    /**
     * @param path Custom event zl path.
     * @return Event node ID.
     */
    static UUID customEventSendNodeId(String path) {
        // <uuid prefix>:<node id>|<seq>
        int startIdx = ZkIgnitePaths.UUID_LEN + 1;

        String idStr = path.substring(startIdx, startIdx + ZkIgnitePaths.UUID_LEN);

        return UUID.fromString(idStr);
    }

    /**
     * @param evtId Event ID.
     * @return Event zk path.
     */
    String joinEventDataPathForJoined(long evtId) {
        return evtsPath + "/joined-" + evtId;
    }

    /**
     * @param evtId Event ID.
     * @return Path for custom event ack.
     */
    String ackEventDataPath(long evtId) {
        return customEventDataPath(true, String.valueOf(evtId));
    }

    /**
     * @param ack Ack event flag.
     * @param child Event child path.
     * @return Full event data path.
     */
    String customEventDataPath(boolean ack, String child) {
        String baseDir = ack ? customEvtsAcksDir : customEvtsDir;

        return baseDir + "/" + child;
    }
}
