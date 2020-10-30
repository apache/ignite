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
public class ZkIgnitePaths {
    /** */
    static final String PATH_SEPARATOR = "/";

    /** */
    private static final byte CLIENT_NODE_FLAG_MASK = 0x01;

    /** */
    private static final int UUID_LEN = 36;

    /** Directory to store joined node data. */
    private static final String JOIN_DATA_DIR = "jd";

    /** Directory to store new custom events. */
    private static final String CUSTOM_EVTS_DIR = "ce";

    /** Directory to store parts of multi-parts custom events. */
    private static final String CUSTOM_EVTS_PARTS_DIR = "cp";

    /** Directory to store acknowledge messages for custom events. */
    private static final String CUSTOM_EVTS_ACKS_DIR = "ca";

    /** Directory to store node's stopped flags. */
    private static final String STOPPED_NODES_FLAGS_DIR = "sf";

    /** Directory to store EPHEMERAL znodes for alive cluster nodes. */
    static final String ALIVE_NODES_DIR = "n";

    /** Path to store discovery events {@link ZkDiscoveryEventsData}. */
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
    final String customEvtsPartsDir;

    /** */
    final String customEvtsAcksDir;

    /** */
    final String stoppedNodesFlagsDir;

    /**
     * @param zkRootPath Base Zookeeper directory for all Ignite nodes.
     */
    ZkIgnitePaths(String zkRootPath) {
        clusterDir = zkRootPath;

        aliveNodesDir = zkPath(ALIVE_NODES_DIR);
        joinDataDir = zkPath(JOIN_DATA_DIR);
        evtsPath = zkPath(DISCO_EVENTS_PATH);
        customEvtsDir = zkPath(CUSTOM_EVTS_DIR);
        customEvtsPartsDir = zkPath(CUSTOM_EVTS_PARTS_DIR);
        customEvtsAcksDir = zkPath(CUSTOM_EVTS_ACKS_DIR);
        stoppedNodesFlagsDir = zkPath(STOPPED_NODES_FLAGS_DIR);
    }

    /**
     * @param path Relative path.
     * @return Full path.
     */
    private String zkPath(String path) {
        return join(clusterDir, path);
    }

    /**
     * @param nodeId Node ID.
     * @param prefixId Unique prefix ID.
     * @return Path.
     */
    String joiningNodeDataPath(UUID nodeId, UUID prefixId) {
        return join(joinDataDir, prefixId + ":" + nodeId.toString());
    }

    /**
     * @param path Alive node zk path.
     * @return Node internal ID.
     */
    static long aliveInternalId(String path) {
        int idx = path.lastIndexOf('|');

        return Long.parseLong(path.substring(idx + 1));
    }

    /**
     * @param prefix Node unique path prefix.
     * @param node Node.
     * @return Path.
     */
    String aliveNodePathForCreate(String prefix, ZookeeperClusterNode node) {
        byte flags = 0;

        if (node.isClient())
            flags |= CLIENT_NODE_FLAG_MASK;

        return join(aliveNodesDir, prefix + ":" + node.id() + ":" + encodeFlags(flags) + "|");
    }

    /**
     * @param path Alive node zk path.
     * @return {@code True} if node is client.
     */
    static boolean aliveNodeClientFlag(String path) {
        return (aliveFlags(path) & CLIENT_NODE_FLAG_MASK) != 0;
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
        // <uuid prefix>:<node id>:<flags>|<alive seq>
        int startIdx = ZkIgnitePaths.UUID_LEN + 1;

        String idStr = path.substring(startIdx, startIdx + ZkIgnitePaths.UUID_LEN);

        return UUID.fromString(idStr);
    }

    /**
     * @param node Leaving node.
     * @return Stopped node path.
     */
    String nodeStoppedFlag(ZookeeperClusterNode node) {
        String path = node.id().toString() + '|' + node.internalId();

        return join(stoppedNodesFlagsDir, path);
    }

    /**
     * @param path Leaving flag path.
     * @return Stopped node internal id.
     */
    static long stoppedFlagNodeInternalId(String path) {
        int idx = path.lastIndexOf('|');

        return Long.parseLong(path.substring(idx + 1));
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
        // <uuid prefix>:<node id>:<partCnt>|<seq>
        int startIdx = ZkIgnitePaths.UUID_LEN + 1;

        String idStr = path.substring(startIdx, startIdx + ZkIgnitePaths.UUID_LEN);

        return UUID.fromString(idStr);
    }

    /**
     * @param path Event path.
     * @return Event unique prefix.
     */
    static String customEventPrefix(String path) {
        // <uuid prefix>:<node id>:<partCnt>|<seq>

        return path.substring(0, ZkIgnitePaths.UUID_LEN);
    }

    /**
     * @param path Custom event zl path.
     * @return Event node ID.
     */
    static int customEventPartsCount(String path) {
        // <uuid prefix>:<node id>:<partCnt>|<seq>
        int startIdx = 2 * ZkIgnitePaths.UUID_LEN + 2;

        String cntStr = path.substring(startIdx, startIdx + 4);

        int partCnt = Integer.parseInt(cntStr);

        assert partCnt >= 1 : partCnt;

        return partCnt;
    }

    /**
     * @param prefix Prefix.
     * @param nodeId Node ID.
     * @param partCnt Parts count.
     * @return Path.
     */
    String createCustomEventPath(String prefix, UUID nodeId, int partCnt) {
        return join(customEvtsDir, prefix + ":" + nodeId + ":" + String.format("%04d", partCnt) + '|');
    }

    /**
     * @param prefix Prefix.
     * @param nodeId Node ID.
     * @return Path.
     */
    String customEventPartsBasePath(String prefix, UUID nodeId) {
        return join(customEvtsPartsDir, prefix + ":" + nodeId + ":");
    }

    /**
     * @param prefix Prefix.
     * @param nodeId Node ID.
     * @param part Part number.
     * @return Path.
     */
    String customEventPartPath(String prefix, UUID nodeId, int part) {
        return customEventPartsBasePath(prefix, nodeId) + String.format("%04d", part);
    }

    /**
     * @param evtId Event ID.
     * @return Event zk path.
     */
    String joinEventDataPathForJoined(long evtId) {
        return join(evtsPath,"fj-" + evtId);
    }

    /**
     * @param topVer Event topology version.
     * @return Event zk path.
     */
    String joinEventSecuritySubjectPath(long topVer) {
        return join(evtsPath, "s-" + topVer);
    }

    /**
     * @param origEvtId ID of original custom event.
     * @return Path for custom event ack.
     */
    String ackEventDataPath(long origEvtId) {
        assert origEvtId != 0;

        return join(customEvtsAcksDir, String.valueOf(origEvtId));
    }

    /**
     * @param id Future ID.
     * @return Future path.
     */
    String distributedFutureBasePath(UUID id) {
        return join(evtsPath, "f-" + id);
    }

    /**
     * @param id Future ID.
     * @return Future path.
     */
    String distributedFutureResultPath(UUID id) {
        return join(evtsPath, "fr-" + id);
    }

    /**
     * @param flags Flags.
     * @return Flags string.
     */
    private static String encodeFlags(byte flags) {
        int intVal = flags + 128;

        String str = Integer.toString(intVal, 16);

        if (str.length() == 1)
            str = '0' + str;

        assert str.length() == 2 : str;

        return str;
    }

    /**
     * @param path Alive node zk path.
     * @return Flags.
     */
    private static byte aliveFlags(String path) {
        int startIdx = path.lastIndexOf(':') + 1;

        String flagsStr = path.substring(startIdx, startIdx + 2);

        return (byte)(Integer.parseInt(flagsStr, 16) - 128);
    }

    /**
     * @param paths Paths to join.
     * @return Paths joined with separator.
     */
    public static String join(String... paths) {
        return String.join(PATH_SEPARATOR, paths);
    }

    /**
     * Validate the provided znode path string.
     *
     * @param path znode path string.
     * @return The given path if it was valid, for fluent chaining.
     * @throws IllegalArgumentException if the path is invalid/
     */
    public static String validatePath(String path) throws IllegalArgumentException {
        if (path == null)
            throw new IllegalArgumentException("Path cannot be null");

        if (path.isEmpty())
            throw new IllegalArgumentException("Path length must be > 0");

        if (path.charAt(0) != '/')
            throw new IllegalArgumentException("Path must start with / character");

        if (path.length() == 1)
            return path;

        if (path.charAt(path.length() - 1) == '/')
            throw new IllegalArgumentException("Path must not end with / character");

        String reason = null;
        char prev = '/';
        char chars[] = path.toCharArray();
        char c;

        for (int i = 1; i < chars.length; prev = chars[i], i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;

                break;
            }
            else if (c == '/' && prev == '/') {
                reason = "empty node name specified @" + i;

                break;
            }
            else if (c == '.' && (i + 1 == chars.length || chars[i + 1] == '/')
                && ((chars[i - 2] == '/' && prev == '.') || chars[i - 1] == '/')) {
                reason = "relative paths not allowed @" + i;

                break;
            }
            else if (c > '\u0000' && c < '\u001f' || c > '\u007f' && c < '\u009f' || c > '\ud800' && c < '\uf8ff'
                || c > '\ufff0' && c < '\uffff') {
                reason = "invalid charater @" + i;

                break;
            }
        }

        if (reason != null)
            throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason);

        return path;
    }
}
