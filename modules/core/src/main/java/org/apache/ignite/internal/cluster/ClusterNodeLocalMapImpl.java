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

package org.apache.ignite.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation for node-local storage.
 * <p>
 * {@code ClusterNodeLocalMapImpl} is similar to {@link ThreadLocal} in a way that its values are not
 * distributed and kept only on local node (similar like {@link ThreadLocal} values are attached to the
 * current thread only). Node-local values are used primarily by jobs executed from the remote
 * nodes to keep intermediate state on the local node between executions.
 * <p>
 * {@code ClusterNodeLocalMapImpl} is a {@link ConcurrentMap} so it is trivial to use.
 * <p>
 * You can get an instance of {@code ClusterNodeLocalMapImpl} by calling {@link IgniteCluster#nodeLocalMap()} method.
 */
public class ClusterNodeLocalMapImpl<K, V> extends ConcurrentHashMap<K, V> implements ConcurrentMap<K, V>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ThreadLocal<String> stash = new ThreadLocal<>();

    /** */
    private GridKernalContext ctx;

    /**
     * No-arg constructor is required by externalization.
     */
    public ClusterNodeLocalMapImpl() {
        // No-op.
    }

    /**
     *
     * @param ctx Kernal context.
     */
    ClusterNodeLocalMapImpl(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stash.set(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            return IgnitionEx.localIgnite().cluster().nodeLocalMap();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNodeLocalMapImpl.class, this);
    }
}
