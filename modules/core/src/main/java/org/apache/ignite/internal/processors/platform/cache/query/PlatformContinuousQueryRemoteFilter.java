/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.cache.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Continuous query filter deployed on remote nodes.
 */
public class PlatformContinuousQueryRemoteFilter implements PlatformContinuousQueryFilter, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock for concurrency control. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Native filter in serialized form. */
    private Object filter;

    /** Grid hosting the filter. */
    @IgniteInstanceResource
    private transient Ignite grid;

    /** Native platform pointer. */
    private transient volatile long ptr;

    /** Close flag. Once set, none requests to native platform is possible. */
    private transient boolean closed;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformContinuousQueryRemoteFilter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param filter Serialized native filter.
     */
    public PlatformContinuousQueryRemoteFilter(Object filter) {
        assert filter != null;

        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent evt) throws CacheEntryListenerException {
        long ptr0 = ptr;

        if (ptr0 == 0)
            deploy();

        lock.readLock().lock();

        try {
            if (closed)
                throw new CacheEntryListenerException("Failed to evaluate the filter because it has been closed.");

            PlatformContext platformCtx = PlatformUtils.platformContext(grid);

            return PlatformUtils.evaluateContinuousQueryEvent(platformCtx, ptr, evt);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Deploy filter to native platform.
     */
    private void deploy() {
        lock.writeLock().lock();

        try {
            // 1. Do not deploy if the filter has been closed concurrently.
            if (closed)
                throw new CacheEntryListenerException("Failed to deploy the filter because it has been closed.");

            // 2. Deploy.
            PlatformContext ctx = PlatformUtils.platformContext(grid);

            try (PlatformMemory mem = ctx.memory().allocate()) {
                PlatformOutputStream out = mem.output();

                BinaryRawWriterEx writer = ctx.writer(out);

                writer.writeObject(filter);

                out.synchronize();

                ptr = ctx.gateway().continuousQueryFilterCreate(mem.pointer());
            }
            catch (Exception e) {
                // 3. Close in case of failure.
                close();

                throw new CacheEntryListenerException("Failed to deploy the filter.", e);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onQueryUnregister() {
        lock.writeLock().lock();

        try {
            close();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close the filter.
     */
    private void close() {
        if (!closed) {
            try {
                if (ptr != 0) {
                    try {
                        PlatformUtils.platformContext(grid).gateway().continuousQueryFilterRelease(ptr);
                    }
                    finally {
                        // Nullify the pointer in any case.
                        ptr = 0;
                    }
                }
            }
            finally {
                closed = true;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(filter);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        filter = in.readObject();

        assert filter != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformContinuousQueryRemoteFilter.class, this);
    }
}
