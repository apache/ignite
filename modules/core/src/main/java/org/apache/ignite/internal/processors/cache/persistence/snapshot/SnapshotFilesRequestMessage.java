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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class SnapshotFilesRequestMessage extends AbstractSnapshotMessage {
    /** Snapshot request message type (value is {@code 178}). */
    public static final short TYPE_CODE = 178;

    /** Snapshot operation request ID. */
    @Order(value = 1, method = "requestId")
    private UUID reqId;

    /** Snapshot name to request. */
    @Order(value = 2, method = "snapshotName")
    private String snpName;

    /** Snapshot directory path. */
    @Order(value = 3, method = "snapshotPath")
    private String snpPath;

    /** Map of cache group ids and corresponding set of its partition ids. */
    @Order(value = 4, method = "partitions")
    private Map<Integer, int[]> parts;

    /**
     * Empty constructor.
     */
    public SnapshotFilesRequestMessage() {
        // No-op.
    }

    /**
     * @param msgId Unique message id.
     * @param reqId Snapshot operation request ID.
     * @param snpName Snapshot name to request.
     * @param snpPath Snapshot directory path.
     * @param parts Map of cache group ids and corresponding set of its partition ids to be snapshot.
     */
    public SnapshotFilesRequestMessage(
        String msgId,
        UUID reqId,
        String snpName,
        @Nullable String snpPath,
        Map<Integer, Set<Integer>> parts
    ) {
        super(msgId);

        assert parts != null && !parts.isEmpty();

        this.reqId = reqId;
        this.snpName = snpName;
        this.snpPath = snpPath;
        this.parts = new HashMap<>();

        for (Map.Entry<Integer, Set<Integer>> e : F.view(parts.entrySet(), e -> !F.isEmpty(e.getValue())))
            this.parts.put(e.getKey(), U.toIntArray(e.getValue()));
    }

    /**
     * @return The demanded cache group partitions per each cache group.
     */
    public Map<Integer, Set<Integer>> parts() {
        Map<Integer, Set<Integer>> res = new HashMap<>();

        for (Map.Entry<Integer, int[]> e : parts.entrySet())
            res.put(e.getKey(), Arrays.stream(e.getValue()).boxed().collect(Collectors.toSet()));

        return res;
    }

    /**
     * @return The demanded cache group partitions per each cache group.
     */
    public Map<Integer, int[]> partitions() {
        return parts;
    }

    /**
     * @param parts The demanded cache group partitions per each cache group.
     */
    public void partitions(Map<Integer, int[]> parts) {
        this.parts = parts;
    }

    /**
     * @return Requested snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @param snpName Requested snapshot name.
     */
    public void snapshotName(String snpName) {
        this.snpName = snpName;
    }

    /**
     * @return Snapshot directory path.
     */
    public String snapshotPath() {
        return snpPath;
    }

    /**
     * @param snpPath Snapshot directory path.
     */
    public void snapshotPath(String snpPath) {
        this.snpPath = snpPath;
    }

    /**
     * @return Snapshot operation request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @param reqId Snapshot operation request ID.
     */
    public void requestId(UUID reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotFilesRequestMessage.class, this, super.toString());
    }
}
