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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Count down latch value.
 */
public final class GridCacheCountDownLatchValue implements GridCacheInternal, Externalizable, Cloneable {
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

    /**
     * Constructor.
     *
     * @param cnt Initial count.
     * @param del {@code True} to auto delete on count down to 0.
     */
    public GridCacheCountDownLatchValue(int cnt, boolean del) {
        assert cnt >= 0;

        this.cnt = cnt;

        initCnt = cnt;

        autoDel = del;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheCountDownLatchValue() {
        // No-op.
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
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        initCnt = in.readInt();
        autoDel = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCountDownLatchValue.class, this);
    }
}