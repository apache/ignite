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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Internal key used to track if sampling enabled or disabled for particular IGFS instance.
 */
class IgfsSamplingKey implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private String name;

    /**
     * Default constructor.
     *
     * @param name - IGFS name.
     */
    IgfsSamplingKey(String name) {
        this.name = name;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgfsSamplingKey() {
        // No-op.
    }

    /**
     * @return IGFS name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return this == obj || (obj instanceof IgfsSamplingKey && F.eq(name, ((IgfsSamplingKey)obj).name));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        name = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsSamplingKey.class, this);
    }
}