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
