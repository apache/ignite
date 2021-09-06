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
import org.apache.ignite.internal.processors.cache.persistence.AbstractCorruptedPersistenceException;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 * Exception to distinguish {@link BPlusTree} tree broken invariants.
 */
public class CorruptedTreeException extends AbstractCorruptedPersistenceException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id of potentially corrupted pages.
     * @param grpName Group name of potentially corrupted pages.
     * @param pageIds Potentially corrupted pages.
     */
    public CorruptedTreeException(String msg, @Nullable Throwable cause, int grpId, String grpName, long... pageIds) {
        this(msg, null, null, grpName, cause, toPagesArray(grpId, pageIds));
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id of potentially corrupted pages.
     * @param grpName Group name of potentially corrupted pages.
     * @param cacheName Cache name.
     * @param indexName Index name.
     * @param pageIds Potentially corrupted pages.
     */
    public CorruptedTreeException(
        String msg,
        @Nullable Throwable cause,
        int grpId,
        String grpName,
        String cacheName,
        String indexName,
        long... pageIds
    ) {
        this(msg, cacheName, indexName, grpName, cause, toPagesArray(grpId, pageIds));
    }

    /**
     * @param msg Message.
     * @param cacheName Cache name.
     * @param indexName Index name.
     * @param grpName Cache group name.
     * @param cause Cause.
     * @param pages (groupId, pageId) pairs for pages that might be corrupted.
     */
    public CorruptedTreeException(
        String msg,
        String cacheName,
        String indexName,
        String grpName,
        @Nullable Throwable cause,
        T2<Integer, Long>... pages
    ) {
        super(getMsg(msg, cacheName, indexName, grpName, pages), cause, pages);
    }

    /** */
    private static String getMsg(String msg, String cacheName, String indexName, String grpName, T2<Integer, Long>... pages) {
        GridStringBuilder stringBuilder = new GridStringBuilder("B+Tree is corrupted [")
            .a("pages(groupId, pageId)=").a(Arrays.toString(pages));

        if (cacheName != null) {
            stringBuilder
                .a(", cacheId=").a(CU.cacheId(cacheName))
                .a(", cacheName=").a(cacheName);
        }

        if (indexName != null)
            stringBuilder.a(", indexName=").a(indexName);

        if (grpName != null)
            stringBuilder.a(", groupName=").a(grpName);

        stringBuilder.a(", msg=").a(msg).a("]");

        return stringBuilder.toString();
    }
}
