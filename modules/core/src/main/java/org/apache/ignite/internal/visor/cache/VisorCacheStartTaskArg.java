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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Cache start arguments.
 */
public class VisorCacheStartTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean near;

    /** */
    private String name;

    /** */
    private String cfg;

    /**
     * Default constructor.
     */
    public VisorCacheStartTaskArg() {
        // No-op.
    }

    /**
     * @param near {@code true} if near cache should be started.
     * @param name Name for near cache.
     * @param cfg Cache XML configuration.
     */
    public VisorCacheStartTaskArg(boolean near, String name, String cfg) {
        this.near = near;
        this.name = name;
        this.cfg = cfg;
    }

    /**
     * @return {@code true} if near cache should be started.
     */
    public boolean isNear() {
        return near;
    }

    /**
     * @return Name for near cache.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Cache XML configuration.
     */
    public String getConfiguration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(near);
        U.writeString(out, name);
        U.writeString(out, cfg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        near = in.readBoolean();
        name = U.readString(in);
        cfg = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheStartTaskArg.class, this);
    }
}
