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

package org.apache.ignite.internal.processors.hadoop.counter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Default Hadoop counter implementation.
 */
public abstract class HadoopCounterAdapter implements HadoopCounter, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter group name. */
    private String grp;

    /** Counter name. */
    private String name;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    protected HadoopCounterAdapter() {
        // No-op.
    }

    /**
     * Creates new counter with given group and name.
     *
     * @param grp Counter group name.
     * @param name Counter name.
     */
    protected HadoopCounterAdapter(String grp, String name) {
        assert grp != null : "counter must have group";
        assert name != null : "counter must have name";

        this.grp = grp;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String group() {
        return grp;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(grp);
        out.writeUTF(name);
        writeValue(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        grp = in.readUTF();
        name = in.readUTF();
        readValue(in);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HadoopCounterAdapter cntr = (HadoopCounterAdapter)o;

        if (!grp.equals(cntr.grp))
            return false;
        if (!name.equals(cntr.name))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = grp.hashCode();
        res = 31 * res + name.hashCode();
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopCounterAdapter.class, this);
    }

    /**
     * Writes value of this counter to output.
     *
     * @param out Output.
     * @throws IOException If failed.
     */
    protected abstract void writeValue(ObjectOutput out) throws IOException;

    /**
     * Read value of this counter from input.
     *
     * @param in Input.
     * @throws IOException If failed.
     */
    protected abstract void readValue(ObjectInput in) throws IOException;
}