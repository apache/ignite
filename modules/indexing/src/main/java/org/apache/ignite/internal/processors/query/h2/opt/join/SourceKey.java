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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.UUID;

/**
 * Key for source.
 */
public class SourceKey {
    /** */
    private final UUID ownerId;

    /** */
    private final int segmentId;

    /** */
    private final int batchLookupId;

    /**
     * @param ownerId Owner node ID.
     * @param segmentId Index segment ID.
     * @param batchLookupId Batch lookup ID.
     */
    public SourceKey(UUID ownerId, int segmentId, int batchLookupId) {
        this.ownerId = ownerId;
        this.segmentId = segmentId;
        this.batchLookupId = batchLookupId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof SourceKey))
            return false;

        SourceKey srcKey = (SourceKey)o;

        return batchLookupId == srcKey.batchLookupId && segmentId == srcKey.segmentId &&
            ownerId.equals(srcKey.ownerId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hash = ownerId.hashCode();

        hash = 31 * hash + segmentId;
        hash = 31 * hash + batchLookupId;

        return hash;
    }
}
