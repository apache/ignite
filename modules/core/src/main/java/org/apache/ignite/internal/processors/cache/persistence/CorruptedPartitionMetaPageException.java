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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * Exception to distinguish partition meta page broken invariants.
 */
public class CorruptedPartitionMetaPageException extends AbstractCorruptedPersistenceException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg   Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pages Ids of pages that might be corrupted.
     */
    protected CorruptedPartitionMetaPageException(String msg, @Nullable Throwable cause, int grpId, long... pages) {
        this(msg, cause, toPagesArray(grpId, pages));
    }

    /**
     * @param msg   Message.
     * @param cause Cause.
     * @param pages (groupId, pageId) pairs for pages that might be corrupted.
     */
    protected CorruptedPartitionMetaPageException(String msg,
        @Nullable Throwable cause,
        T2<Integer, Long>[] pages
    ) {
        super(msg, cause, pages);
    }
}
