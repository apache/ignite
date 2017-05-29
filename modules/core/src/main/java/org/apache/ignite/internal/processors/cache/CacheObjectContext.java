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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class CacheObjectContext implements CacheObjectValueContext {
    /** */
    private GridKernalContext kernalCtx;

    /** */
    private String cacheName;

    /** */
    private AffinityKeyMapper dfltAffMapper;

    /** */
    private boolean cpyOnGet;

    /** */
    private boolean storeVal;

    /** */
    private boolean addDepInfo;

    /**
     * @param kernalCtx Kernal context.
     * @param dfltAffMapper Default affinity mapper.
     * @param cpyOnGet Copy on get flag.
     * @param storeVal {@code True} if should store unmarshalled value in cache.
     * @param addDepInfo {@code true} if deployment info should be associated with the objects of this cache.
     */
    public CacheObjectContext(GridKernalContext kernalCtx,
        String cacheName,
        AffinityKeyMapper dfltAffMapper,
        boolean cpyOnGet,
        boolean storeVal,
        boolean addDepInfo) {
        this.kernalCtx = kernalCtx;
        this.cacheName = cacheName;
        this.dfltAffMapper = dfltAffMapper;
        this.cpyOnGet = cpyOnGet;
        this.storeVal = storeVal;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public boolean copyOnGet() {
        return cpyOnGet;
    }

    /** {@inheritDoc} */
    @Override public boolean storeValue() {
        return storeVal;
    }

    /**
     * @return Default affinity mapper.
     */
    public AffinityKeyMapper defaultAffMapper() {
        return dfltAffMapper;
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext kernalContext() {
        return kernalCtx;
    }

    /** {@inheritDoc} */
    @Override public boolean binaryEnabled() {
        return false;
    }

    /**
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public Object unwrapBinaryIfNeeded(Object o, boolean keepBinary, boolean cpy) {
        if (o == null)
            return null;

        return CacheObjectUtils.unwrapBinaryIfNeeded(this, o, keepBinary, cpy);
    }
}
