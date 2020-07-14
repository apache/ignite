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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClosableIteratorsHolder {
    /** */
    private final ReferenceQueue refQueue;

    /** */
    private final Map<Reference, Object> refMap;

    /** */
    private final IgniteLogger log;

    /** */
    private volatile boolean stopped;

    /** */
    private Thread cleanWorker;

    /** */
    public ClosableIteratorsHolder(IgniteLogger log) {
        this.log = log;

        refQueue = new ReferenceQueue<>();
        refMap = new ConcurrentHashMap<>();
    }

    /**
     * @param src Closeable iterator.
     * @return Weak closable iterator wrapper.
     */
    public <T> Iterator<T> iterator(final Iterator<T> src) {
        cleanUp(false);

        return new DelegatingIterator<>(src);
    }

    /** */
    public void init() {
        cleanWorker = new Thread(() -> cleanUp(true));
        cleanWorker.setDaemon(true);
        cleanWorker.start();
    }

    /** */
    public void tearDown() {
        stopped = true;
        refMap.clear();
        U.interrupt(cleanWorker);
    }

    /** */
    private void cleanUp(boolean blocking) {
        for (Reference<?> ref = nextRef(blocking); !stopped && ref != null; ref = nextRef(blocking))
            Commons.close(refMap.remove(ref), log);
    }

    /** */
    private Reference nextRef(boolean blocking) {
        try {
            return !blocking ? refQueue.poll() : refQueue.remove();
        }
        catch (InterruptedException ignored) {
            return null;
        }
    }

    /** */
    private AutoCloseable closeable(Object referent, Object resource) {
        if (!(resource instanceof AutoCloseable))
            return null;

        return new CloseableReference(referent, resource);
    }

    /** */
    private final class DelegatingIterator<T> implements Iterator<T>, AutoCloseable {
        /** */
        private final Iterator<T> delegate;

        /** */
        private final AutoCloseable closeable;

        /** */
        private DelegatingIterator(Iterator<T> delegate) {
            closeable = closeable(this, this.delegate = delegate);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return delegate.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
            return delegate.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            delegate.remove();
        }

        /** {@inheritDoc} */
        @Override public void forEachRemaining(Consumer<? super T> action) {
            delegate.forEachRemaining(action);
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            Commons.close(closeable);
        }
    }

    /** */
    private final class CloseableReference extends WeakReference implements AutoCloseable {
        /** */
        private CloseableReference(Object referent, Object resource) {
            super(referent, refQueue);

            refMap.put(this, resource);
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            try {
                Commons.close(refMap.remove(this));
            }
            finally {
                clear();
            }
        }
    }
}
