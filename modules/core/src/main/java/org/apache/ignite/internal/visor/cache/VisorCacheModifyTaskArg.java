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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorCacheModifyTask}.
 */
public class VisorCacheModifyTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Modification mode. */
    private VisorModifyCacheMode mode;

    /** Specified key. */
    private Object key;

    /** Specified value. */
    private Object val;

    /**
     * Default constructor.
     */
    public VisorCacheModifyTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param mode Modification mode.
     * @param key Specified key.
     * @param val Specified value.
     */
    public VisorCacheModifyTaskArg(String cacheName, VisorModifyCacheMode mode, Object key, Object val) {
        this.cacheName = cacheName;
        this.mode = mode;
        this.key = key;
        this.val = val;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Modification mode.
     */
    public VisorModifyCacheMode getMode() {
        return mode;
    }

    /**
     * @return Specified key.
     */
    public Object getKey() {
        return key;
    }

    /**
     * @return Specified value.
     */
    public Object getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeEnum(out, mode);
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        mode = VisorModifyCacheMode.fromOrdinal(in.readByte());
        key = in.readObject();
        val = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheModifyTaskArg.class, this);
    }
}
