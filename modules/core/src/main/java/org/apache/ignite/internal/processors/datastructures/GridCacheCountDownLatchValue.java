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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Count down latch value.
 */
public final class GridCacheCountDownLatchValue extends VolatileAtomicDataStructureValue implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count. */
    @GridToStringInclude(sensitive = true)
    private int cnt;

    /** Initial count. */
    @GridToStringInclude(sensitive = true)
    private int initCnt;

    /** Auto delete flag. */
    private boolean autoDel;

    /** */
    private long gridStartTime;

    /**
     * Constructor.
     *
     * @param cnt Initial count.
     * @param del {@code True} to auto delete on count down to 0.
     * @param gridStartTime Cluster start time.
     */
    public GridCacheCountDownLatchValue(int cnt, boolean del, long gridStartTime) {
        assert cnt >= 0;

        this.cnt = cnt;

        initCnt = cnt;

        autoDel = del;

        this.gridStartTime = gridStartTime;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheCountDownLatchValue() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.COUNT_DOWN_LATCH;
    }

    /** {@inheritDoc} */
    @Override public long gridStartTime() {
        return gridStartTime;
    }

    /**
     * @param cnt New count.
     */
    public void set(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Current count.
     */
    public int get() {
        return cnt;
    }

    /**
     * @return Initial count.
     */
    public int initialCount() {
        return initCnt;
    }

    /**
     * @return Auto-delete flag.
     */
    public boolean autoDelete() {
        return autoDel;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cnt);
        out.writeInt(initCnt);
        out.writeBoolean(autoDel);
        out.writeLong(gridStartTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        initCnt = in.readInt();
        autoDel = in.readBoolean();
        gridStartTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCountDownLatchValue.class, this);
    }
}