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

package org.apache.ignite.internal.util;

import java.util.Collections;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Striped pool.
 */
public abstract class GridStripedPool<T, E extends Exception> implements AutoCloseable {
    /** */
    private final Queue<T>[] stripes;

    /** */
    private final Set<T> allCreatedInstances = Collections.newSetFromMap(
        new ConcurrentHashMap<T,Boolean>());

    /** */
    private final int maxPickAttempts;

    /** */
    private final AtomicBoolean closed = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    public GridStripedPool(int stripesCnt, int maxPickAttempts) {
        assert stripesCnt > 0: stripesCnt;
        assert maxPickAttempts > 0: maxPickAttempts;

        this.maxPickAttempts = maxPickAttempts;

        Queue<T>[] s = new Queue[stripesCnt];

        for (int i = 0; i < stripesCnt; i++)
            s[i] = new LinkedBlockingQueue<>();

        stripes = s;
    }

    /**
     * @return {@code true} If the pool is closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * @return Number of objects in this pool.
     */
    public int getPoolSize() {
        int size = 0;

        for (Queue<T> stripe : stripes)
            size += stripe.size();

        return size;
    }

    /** {@inheritDoc} */
    @Override public void close() throws E {
        if (closed.compareAndSet(false, true)) {
            for (Queue<T> queue : stripes)
                closeStripe(queue);

            if (!allCreatedInstances.isEmpty()) {
                for (T o : allCreatedInstances)
                    doDestroy(o);
            }

            assert allCreatedInstances.isEmpty();
        }
    }

    /**
     * @param queue Stripe.
     * @throws E If failed.
     */
    private void closeStripe(Queue<T> queue) throws E {
        for (;;) {
            T o = queue.poll();

            if (o == null)
                break;

            doDestroy(o);
        }
    }

    /**
     * @param rnd Random.
     * @return Random stripe.
     */
    private Queue<T> randomStripe(Random rnd) {
        return stripes[rnd.nextInt(stripes.length)];
    }

    /**
     * Take a pooled or newly created instance.
     *
     * @return Pooled or newly created instance.
     * @throws E If failed.
     */
    public T take() throws E {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        T res;

        for (int i = 0; i < maxPickAttempts; i++) {
            res = randomStripe(rnd).poll();

            if (res != null) {
                if (validate(res))
                    return res;

                doDestroy(res);
            }
        }

        res = create();

        if (res == null || !validate(res) || closed.get()) {
            if (res != null)
                doDestroy(res);

            throw new IllegalStateException();
        }

        return res;
    }

    /**
     * @param o The pooled object to destroy.
     * @throws E If failed.
     */
    private void doDestroy(T o) throws E {
        allCreatedInstances.remove(o);

        destroy(o);
    }

    /**
     * @param o Instance to put into this pool.
     * @throws E If failed.
     */
    public void put(T o) throws E {
        assert o != null;

        cleanup(o);

        if (!validate(o)) {
            doDestroy(o);

            return;
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Queue<T> s = randomStripe(rnd);

        if (!s.offer(o))
            throw new IllegalStateException();

        if (closed.get())
            closeStripe(s); // Racy close is allowed.
    }

    /**
     * @param o Instance to validate.
     * @return {@code true} If the instance is valid.
     * @throws E If failed.
     */
    protected abstract boolean validate(T o) throws E;

    /**
     * @return New instance.
     * @throws E If failed.
     */
    protected abstract T create() throws E ;

    /**
     * @param o Instance to cleanup before returning to the pool.
     * @throws E If failed.
     */
    protected abstract void cleanup(T o) throws E;

    /**
     * @param o The pooled object to destroy.
     * @throws E If failed.
     */
    protected abstract void destroy(T o) throws E ;
}
