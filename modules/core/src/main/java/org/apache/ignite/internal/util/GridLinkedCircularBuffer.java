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
 *
 */

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

public class GridLinkedCircularBuffer<T> implements GridCircularBufferInterface<T> {

    private final AtomicInteger size = new AtomicInteger();

    private final int cap;

    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();

    public GridLinkedCircularBuffer(int cap) {
        this.cap = cap;
    }

    @Override public Collection<T> items() {
        ArrayList<T> result = new ArrayList<>();

        for (T item : queue) {
            result.add(item);
        }

        return result;
    }

    public void clear(IgniteInClosure<T> c) {
        int removed = 0;

        T item = queue.poll();

        while (item != null) {
            removed++;

            c.apply(item);

            item = queue.poll();
        }

        if (removed != 0)
            size.addAndGet(-removed);
    }

    @Override public void forEach(IgniteInClosure<T> c) {
        for (T item : queue) {
            c.apply(item);
        }
    }

    @Nullable @Override public T add(T t) throws InterruptedException {
        queue.add(t);

        if (size.get() > cap) {
            T retVal = queue.poll();
            if (retVal != null)
                return retVal;
        }

        size.incrementAndGet();

        return null;
    }
}
