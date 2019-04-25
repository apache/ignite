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
 * Argument for {@link VisorCacheClearTask}.
 */
public class VisorCacheClearTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /**
     * Default constructor.
     */
    public VisorCacheClearTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     */
    public VisorCacheClearTaskArg(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheClearTaskArg.class, this);
    }
}
