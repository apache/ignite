/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

/**
 *
 */
public interface MvccCoordinatorChangeAware {
    /** */
    AtomicLong ID_CNTR = new AtomicLong();

    /** */
    long MVCC_TRACKER_ID_NA = -1;

    /** */
    LongPredicate ID_FILTER = id -> id != MVCC_TRACKER_ID_NA;

    /**
     * Mvcc coordinator change callback.
     *
     * @param newCrd New mvcc coordinator.
     * @return Query id if exists.
     */
    default long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        return MVCC_TRACKER_ID_NA;
    }
}
