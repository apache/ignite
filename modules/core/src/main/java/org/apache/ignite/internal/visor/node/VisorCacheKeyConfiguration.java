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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for configuration of cache key data structures.
 */
public class VisorCacheKeyConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name. */
    private String typeName;

    /** Affinity key field name. */
    private String affKeyFieldName;

    /**
     * Construct data transfer object for cache key configurations properties.
     *
     * @param cfgs Cache key configurations.
     * @return Cache key configurations properties.
     */
    public static List<VisorCacheKeyConfiguration> list(CacheKeyConfiguration[] cfgs) {
        List<VisorCacheKeyConfiguration> res = new ArrayList<>();

        if (!F.isEmpty(cfgs)) {
            for (CacheKeyConfiguration cfg : cfgs)
                res.add(new VisorCacheKeyConfiguration(cfg));
        }

        return res;
    }

    /**
     * Default constructor.
     */
    public VisorCacheKeyConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache key configuration.
     *
     * @param src Cache key configuration.
     */
    public VisorCacheKeyConfiguration(CacheKeyConfiguration src) {
        typeName = src.getTypeName();
        affKeyFieldName = src.getAffinityKeyFieldName();
    }

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @return Affinity key field name.
     */
    public String getAffinityKeyFieldName() {
        return affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, typeName);
        U.writeString(out, affKeyFieldName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        typeName = U.readString(in);
        affKeyFieldName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheKeyConfiguration.class, this);
    }
}
