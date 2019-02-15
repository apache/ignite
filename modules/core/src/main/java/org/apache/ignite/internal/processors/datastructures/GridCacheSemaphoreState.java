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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Grid cache semaphore state.
 */
public class GridCacheSemaphoreState extends VolatileAtomicDataStructureValue implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Permission count. */
    private int cnt;

    /** Map containing number of acquired permits for each node waiting on this semaphore. */
    @GridToStringInclude
    private Map<UUID, Integer> waiters;

    /** FailoverSafe flag. */
    private boolean failoverSafe;

    /** Flag indicating that semaphore is no longer safe to use. */
    private boolean broken;

    /** */
    private long gridStartTime;

    /**
     * Constructor.
     *
     * @param cnt Number of permissions.
     * @param waiters Waiters map.
     * @param failoverSafe Failover safe flag.
     * @param gridStartTime Cluster start time.
     */
    public GridCacheSemaphoreState(int cnt, @Nullable Map<UUID,Integer> waiters, boolean failoverSafe, long gridStartTime) {
        this.cnt = cnt;
        this.waiters = waiters;
        this.failoverSafe = failoverSafe;
        this.gridStartTime = gridStartTime;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheSemaphoreState() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.SEMAPHORE;
    }

    /** {@inheritDoc} */
    @Override public long gridStartTime() {
        return gridStartTime;
    }

    /**
     * @param cnt New count.
     */
    public void setCount(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Current count.
     */
    public int getCount() {
        return cnt;
    }

    /**
     * @return Waiters.
     */
    public Map<UUID,Integer> getWaiters() {
        return waiters;
    }

    /**
     * @param waiters Map containing the number of permissions acquired by each node.
     */
    public void setWaiters(Map<UUID, Integer> waiters) {
        this.waiters = waiters;
    }

    /**
     * @return failoverSafe flag.
     */
    public boolean isFailoverSafe() {
        return failoverSafe;
    }

    /**
     * @return broken flag.
     */
    public boolean isBroken() {
        return broken;
    }

    /**
     *
     * @param broken Flag indicating that this semaphore should be no longer used.
     */
    public void setBroken(boolean broken) {
        this.broken = broken;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cnt);
        out.writeBoolean(failoverSafe);
        out.writeLong(gridStartTime);
        out.writeBoolean(waiters != null);

        if (waiters != null) {
            out.writeInt(waiters.size());

            for (Map.Entry<UUID, Integer> e : waiters.entrySet()) {
                U.writeUuid(out, e.getKey());
                out.writeInt(e.getValue());
            }
        }

        out.writeBoolean(broken);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        failoverSafe = in.readBoolean();
        gridStartTime = in.readLong();

        if (in.readBoolean()) {
            int size = in.readInt();

            waiters = U.newHashMap(size);

            for (int i = 0; i < size; i++)
                waiters.put(U.readUuid(in), in.readInt());
        }

        broken = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSemaphoreState.class, this);
    }
}
