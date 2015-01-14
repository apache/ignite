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

import java.util.*;

/**
 * Unbounded window which holds all events. Events can be evicted manually from window
 * via any of the {@code dequeue(...)} methods.
 */
public class StreamerUnboundedWindow<E> extends StreamerWindowAdapter<E> {
    /** Events. */
    private ConcurrentLinkedDeque8<E> evts = new ConcurrentLinkedDeque8<>();

    /** {@inheritDoc} */
    @Override protected void stop0() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void checkConfiguration() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void reset0() {
        evts.clear();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return evts.sizex();
    }

    /** {@inheritDoc} */
    @Override protected GridStreamerWindowIterator<E> iterator0() {
        final ConcurrentLinkedDeque8.IteratorEx<E> it = (ConcurrentLinkedDeque8.IteratorEx<E>)evts.iterator();

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
                return (it.removex()) ? lastRet : null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int evictionQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean enqueue0(E evt) {
        return evts.add(evt);
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> dequeue0(int cnt) {
        Collection<E> res = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            E evicted = evts.pollLast();

            if (evicted != null)
                res.add(evicted);
            else
                break;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvicted0(int cnt) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvictedBatch0() {
        return Collections.emptyList();
    }
}
