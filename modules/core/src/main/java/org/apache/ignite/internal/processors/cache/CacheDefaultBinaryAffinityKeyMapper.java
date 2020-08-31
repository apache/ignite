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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheDefaultBinaryAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private CacheObjectBinaryProcessorImpl proc;

    /** Mapping from type name to affinity field name. */
    private Map<String, String> typeNameAffFields = new HashMap<>();

    /** Mapping from type ID to affinity field name. */
    private transient volatile Map<Integer, BinaryField> typeIdAffFields;

    /**
     * Constructor.
     *
     * @param cacheKeyCfgs Cache key configurations.
     */
    public CacheDefaultBinaryAffinityKeyMapper(@Nullable CacheKeyConfiguration[] cacheKeyCfgs) {
        if (!F.isEmpty(cacheKeyCfgs)) {
            for (CacheKeyConfiguration cacheKeyCfg : cacheKeyCfgs)
                typeNameAffFields.put(cacheKeyCfg.getTypeName(), cacheKeyCfg.getAffinityKeyFieldName());
        }
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        try {
            key = proc.toBinary(key, false);
        }
        catch (IgniteException e) {
            U.error(log, "Failed to marshal key to binary: " + key, e);
        }

        if (key instanceof BinaryObject) {
            assert key instanceof BinaryObjectEx : "All BinaryObject implementations must implement " +
                BinaryObjectEx.class.getName() + ": " + key.getClass().getName();

            BinaryObjectEx key0 = (BinaryObjectEx)key;

            BinaryField affField = affinityKeyField(key0.typeId());

            if (affField != null) {
                Object res = affField.value(key0);

                if (res != null)
                    return res;
            }

            return key;
        }
        else
            return super.affinityKey(key);
    }

    /**
     * Get affinity field override for type.
     *
     * @param typeName Type name.
     * @return Affinity field override if any.
     */
    @Nullable public BinaryField affinityKeyField(String typeName) {
        int typeId = proc.typeId(typeName);

        return affinityKeyField(typeId);
    }

    /**
     * Get affinity field override for type.
     *
     * @param typeId Type ID.
     * @return Affinity field override if any.
     */
    @Nullable public BinaryField affinityKeyField(int typeId) {
        Map<Integer, BinaryField> typeIdAffFields0 = typeIdAffFields;

        if (typeIdAffFields0 == null) {
            typeIdAffFields0 = new HashMap<>();

            for (Map.Entry<String, String> entry : typeNameAffFields.entrySet()) {
                String typeName = entry.getKey();
                String affFieldName = entry.getValue();

                int curTypeId = proc.typeId(typeName);

                BinaryField field = proc.binaryContext().createField(curTypeId, affFieldName);

                typeIdAffFields0.put(curTypeId, field);
            }

            typeIdAffFields = typeIdAffFields0;
        }

        BinaryField res = typeIdAffFields0.get(typeId);

        if (res == null)
            res = proc.affinityKeyField(typeId);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void ignite(Ignite ignite) {
        super.ignite(ignite);

        if (ignite != null) {
            IgniteKernal kernal = (IgniteKernal)ignite;

            proc = (CacheObjectBinaryProcessorImpl)kernal.context().cacheObjects();
        }
    }
}
