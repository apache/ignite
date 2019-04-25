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

package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Implementation of observer-observable pattern. For handling specific changes of segment.
 */
public abstract class SegmentObservable {
    /** Observers for handle changes of archived index. */
    private final Queue<Consumer<Long>> observers = new ConcurrentLinkedQueue<>();

    /**
     * @param observer Observer for notification about segment's changes.
     */
    void addObserver(Consumer<Long> observer) {
        observers.add(observer);
    }

    /**
     * Notify observers about changes.
     *
     * @param segmentId Segment which was been changed.
     */
    void notifyObservers(long segmentId) {
        observers.forEach(observer -> observer.accept(segmentId));
    }
}
