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

package org.apache.ignite.internal.processors.hadoop.proto;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Task arguments.
 */
public class HadoopProtocolTaskArguments implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Arguments. */
    private Object[] args;

    /**
     * {@link Externalizable} support.
     */
    public HadoopProtocolTaskArguments() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public HadoopProtocolTaskArguments(Object... args) {
        this.args = args;
    }

    /**
     * @param idx Argument index.
     * @return Argument.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T get(int idx) {
        return (args != null && args.length > idx) ? (T)args[idx] : null;
    }

    /**
     * @return Size.
     */
    public int size() {
        return args != null ? args.length : 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeArray(out, args);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        args = U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopProtocolTaskArguments.class, this);
    }
}