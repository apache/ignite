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

package org.apache.ignite.internal.processors.cache;

import java.util.TreeMap;

/**
 * Update counter implementation for MVCC mode.
 */
public class PartitionUpdateCounterMvccImpl extends PartitionUpdateCounterTrackingImpl {
    /**
     * @param grp Group.
     */
    public PartitionUpdateCounterMvccImpl(CacheGroupContext grp) {
        super(grp);
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        return next(delta);
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return get();
    }

    /** {@inheritDoc} */
    @Override protected PartitionUpdateCounterTrackingImpl createInstance() {
        return new PartitionUpdateCounterMvccImpl(grp);
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter copy() {
        PartitionUpdateCounterMvccImpl copy = new PartitionUpdateCounterMvccImpl(grp);

        copy.lwm.set(lwm.get());
        copy.first = first;
        copy.queue = new TreeMap<>(queue);
        copy.initCntr = initCntr;
        copy.reservedCntr.set(reservedCntr.get());

        return copy;
    }
}
