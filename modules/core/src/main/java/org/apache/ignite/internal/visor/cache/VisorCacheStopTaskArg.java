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
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorCacheStopTask}.
 */
public class VisorCacheStopTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Cache names. */
    private List<String> cacheNames;

    /**
     * Default constructor.
     */
    public VisorCacheStopTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     */
    public VisorCacheStopTaskArg(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @param cacheNames Cache names.
     */
    public VisorCacheStopTaskArg(List<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Cache names.
     */
    public List<String> getCacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);

        if (protoVer > V1)
            cacheNames = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheStopTaskArg.class, this);
    }
}
