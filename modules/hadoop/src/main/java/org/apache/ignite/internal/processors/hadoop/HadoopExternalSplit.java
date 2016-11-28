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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Split serialized in external file.
 */
public class HadoopExternalSplit extends HadoopInputSplit {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long off;

    /**
     * For {@link Externalizable}.
     */
    public HadoopExternalSplit() {
        // No-op.
    }

    /**
     * @param hosts Hosts.
     * @param off Offset of this split in external file.
     */
    public HadoopExternalSplit(String[] hosts, long off) {
        assert off >= 0 : off;
        assert hosts != null;

        this.hosts = hosts;
        this.off = off;
    }

    /**
     * @return Offset of this input split in external file.
     */
    public long offset() {
        return off;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(off);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        off = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        HadoopExternalSplit that = (HadoopExternalSplit) o;

        return off == that.off;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(off ^ (off >>> 32));
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(HadoopExternalSplit.class, this, "hosts", Arrays.toString(hosts));
    }
}