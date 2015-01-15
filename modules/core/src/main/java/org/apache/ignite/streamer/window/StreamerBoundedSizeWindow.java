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

package org.apache.ignite.streamer.window;

import org.gridgain.grid.kernal.processors.streamer.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Queue window bounded by number of elements in queue. After adding elements to this window called
 * must check for evicted events.
 * <p>
 * It is guaranteed that window size will never get less then maximum size when poling from this window
 * concurrently from different threads.
 */
public class StreamerBoundedSizeWindow<E> extends StreamerBoundedSizeWindowAdapter<E, E> {
    /** {@inheritDoc} */
    @Override protected Collection<E> newCollection() {
        return new ConcurrentLinkedDeque8<>();
    }

    /** {@inheritDoc} */
    @Override public GridStreamerWindowIterator<E> iteratorInternal(Collection<E> col, final Set<E> set,
        final AtomicInteger size) {
        final ConcurrentLinkedDeque8.IteratorEx<E> it =
            (ConcurrentLinkedDeque8.IteratorEx<E>)col.iterator();

        return new GridStreamerWindowIterator<E>() {
            private E lastRet;

            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public E next() {
                lastRet = it.next();

                return lastRet;
            }

            @Override public E removex() {
                if (it.removex()) {
                    if (set != null)
                        set.remove(lastRet);

                    size.decrementAndGet();

                    return lastRet;
                }
                else
                    return null;
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override protected boolean addInternal(E evt, Collection<E> col, Set<E> set) {
        assert col instanceof ConcurrentLinkedDeque8;

        // If unique.
        if (set != null) {
            if (set.add(evt)) {
                col.add(evt);

                return true;
            }

            return false;
        }
        else {
            col.add(evt);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override protected int addAllInternal(Collection<E> evts, Collection<E> col, Set<E> set) {
        assert col instanceof ConcurrentLinkedDeque8;
        if (set != null) {
            int cnt = 0;

            for (E evt : evts) {
                if (set.add(evt)) {
                    col.add(evt);

                    cnt++;
                }
            }

            return cnt;
        }
        else {
            col.addAll(evts);

            return evts.size();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected E pollInternal(Collection<E> col, Set<E> set) {
        assert col instanceof ConcurrentLinkedDeque8;

        E res = ((Queue<E>)col).poll();

        if (set != null && res != null)
            set.remove(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void consistencyCheck(Collection<E> col, Set<E> set, AtomicInteger size) {
        assert col.size() == size.get();

        if (set != null) {
            // Check no duplicates in collection.

            Collection<Object> vals = new HashSet<>();

            for (Object evt : col)
                assert vals.add(evt);
        }
    }
}
