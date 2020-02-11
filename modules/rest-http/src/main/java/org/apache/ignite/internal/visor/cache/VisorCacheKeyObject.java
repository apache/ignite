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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for {@link VisorCacheGetValueTask}.
 */
public class VisorCacheKeyObject extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type of key object. */
    private VisorDataType type;

    /** Key value. */
    private Object key;

    /**
     * Default constructor.
     */
    public VisorCacheKeyObject() {
        // No-op.
    }

    /**
     * @param type Type of key object.
     * @param key Specified key.
     */
    public VisorCacheKeyObject(VisorDataType type, Object key) {
        this.type = type;
        this.key = key;
    }

    /**
     * @return Key type.
     */
    public VisorDataType getType() {
        return type;
    }

    /**
     * @return Key.
     */
    public Object getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, type);
        out.writeObject(key);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        type = VisorDataType.fromOrdinal(in.readByte());
        key = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheKeyObject.class, this);
    }
}
