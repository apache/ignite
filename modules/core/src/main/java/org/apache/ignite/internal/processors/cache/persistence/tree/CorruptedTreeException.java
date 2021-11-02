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

package org.apache.ignite.internal.processors.cache.persistence.tree;

import java.util.Arrays;
import org.apache.ignite.internal.processors.cache.persistence.CorruptedDataStructureException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 * Exception to distinguish {@link BPlusTree} tree broken invariants.
 */
public class CorruptedTreeException extends CorruptedDataStructureException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param msg     Message.
     * @param cause   Cause.
     * @param grpName Cache group name.
     * @param grpId   Cache group id.
     * @param pageIds PageId's that can be corrupted.
     */
    public CorruptedTreeException(String msg, @Nullable Throwable cause, String grpName, int grpId, long... pageIds) {
        this(msg, null, null, grpName, cause, grpId, pageIds);
    }

    /**
     * Constructor.
     *
     * @param msg       Message.
     * @param cause     Cause.
     * @param grpName   Group name of potentially corrupted pages.
     * @param cacheName Cache name.
     * @param indexName Index name.
     * @param grpId     Cache group id.
     * @param pageIds   PageId's that can be corrupted.
     */
    public CorruptedTreeException(
        String msg,
        @Nullable Throwable cause,
        String grpName,
        String cacheName,
        String indexName,
        int grpId,
        long... pageIds
    ) {
        this(msg, cacheName, indexName, grpName, cause, grpId, pageIds);
    }

    /**
     * Constructor.
     *
     * @param msg       Message.
     * @param cacheName Cache name.
     * @param indexName Index name.
     * @param grpName   Cache group name.
     * @param cause     Cause.
     * @param grpId     Cache group id.
     * @param pageIds   PageId's that can be corrupted.
     */
    public CorruptedTreeException(
        String msg,
        String cacheName,
        String indexName,
        String grpName,
        @Nullable Throwable cause,
        int grpId,
        long... pageIds
    ) {
        super(getMsg(msg, cacheName, indexName, grpName, grpId, pageIds), cause, grpId, pageIds);
    }

    /**
     * Creates {@link CorruptedTreeException} error message.
     *
     * @param msg       Message.
     * @param cacheName Cache name.
     * @param indexName Index name.
     * @param grpName   Cache group name.
     * @param grpId     Cache group id.
     * @param pageIds   PageId's that can be corrupted.
     * @return Error message.
     */
    private static String getMsg(
        String msg,
        @Nullable String cacheName,
        @Nullable String indexName,
        @Nullable String grpName,
        int grpId,
        long... pageIds
    ) {
        SB sb = new SB("B+Tree is corrupted [groupId=");

        sb.a(grpId).a(", pageIds=").a(Arrays.toString(pageIds));

        if (cacheName != null)
            sb.a(", cacheId=").a(CU.cacheId(cacheName)).a(", cacheName=").a(cacheName);

        if (indexName != null)
            sb.a(", indexName=").a(indexName);

        if (grpName != null)
            sb.a(", groupName=").a(grpName);

        sb.a(", msg=").a(msg).a(']');

        return sb.toString();
    }
}
