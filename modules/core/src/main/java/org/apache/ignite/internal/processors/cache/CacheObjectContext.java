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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened") public class CacheObjectContext {
    /** */
    private GridKernalContext kernalCtx;

    /** */
    private IgniteCacheObjectProcessor proc;

    /** */
    private AffinityKeyMapper dfltAffMapper;

    /** */
    private boolean cpyOnGet;

    /** */
    private boolean storeVal;

    /** */
    private boolean p2pEnabled;

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
        AffinityKeyMapper dfltAffMapper,
        boolean cpyOnGet,
        boolean storeVal,
        boolean addDepInfo) {
        this.kernalCtx = kernalCtx;
        this.dfltAffMapper = dfltAffMapper;
        this.cpyOnGet = cpyOnGet;
        this.storeVal = storeVal;
        this.addDepInfo = addDepInfo;

        p2pEnabled = kernalCtx.config().isPeerClassLoadingEnabled();
        proc = kernalCtx.cacheObjects();
    }

    /**
     * @return {@code True} if peer class loading is enabled.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
    }

    /**
     * @return {@code True} if deployment info should be associated with the objects of this cache.
     */
    public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Copy on get flag.
     */
    public boolean copyOnGet() {
        return cpyOnGet;
    }

    /**
     * @return {@code True} if should store unmarshalled value in cache.
     */
    public boolean storeValue() {
        return storeVal;
    }

    /**
     * @return Default affinity mapper.
     */
    public AffinityKeyMapper defaultAffMapper() {
        return dfltAffMapper;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalCtx;
    }

    /**
     * @return Processor.
     */
    public IgniteCacheObjectProcessor processor() {
        return proc;
    }

    /** {@inheritDoc} */
    public Object unwrapPortableIfNeeded(Object o, boolean keepPortable) {
        return unwrapPortableIfNeeded(o, keepPortable, true);
    }

    /** {@inheritDoc} */
    public Object unwrapPortableIfNeeded(Object o, boolean keepPortable, boolean cpy) {
        if (o == null)
            return null;

        return unwrapPortable(o, keepPortable, cpy);
    }

    /** {@inheritDoc} */
    public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        return unwrapPortablesIfNeeded(col, keepPortable, true);
    }

    /** {@inheritDoc} */
    public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable, boolean cpy) {
        if (col instanceof ArrayList)
            return unwrapPortables((ArrayList<Object>)col, keepPortable, cpy);

        if (col instanceof Set)
            return unwrapPortables((Set<Object>)col, keepPortable, cpy);

        Collection<Object> col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapPortable(obj, keepPortable, cpy));

        return col0;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    private Map<Object, Object> unwrapPortablesIfNeeded(Map<Object, Object> map, boolean keepPortable, boolean cpy) {
        if (keepPortable)
            return map;

        Map<Object, Object> map0 = PortableUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapPortable(e.getKey(), keepPortable, cpy), unwrapPortable(e.getValue(), keepPortable, cpy));

        return map0;
    }

    /**
     * Unwraps array list.
     *
     * @param col List to unwrap.
     * @return Unwrapped list.
     */
    private Collection<Object> unwrapPortables(ArrayList<Object> col, boolean keepPortable, boolean cpy) {
        int size = col.size();

        for (int i = 0; i < size; i++) {
            Object o = col.get(i);

            Object unwrapped = unwrapPortable(o, keepPortable, cpy);

            if (o != unwrapped)
                col.set(i, unwrapped);
        }

        return col;
    }

    /**
     * Unwraps set with binary.
     *
     * @param set Set to unwrap.
     * @return Unwrapped set.
     */
    private Set<Object> unwrapPortables(Set<Object> set, boolean keepPortable, boolean cpy) {
        Set<Object> set0 = PortableUtils.newSet(set);

        for (Object obj : set)
            set0.add(unwrapPortable(obj, keepPortable, cpy));

        return set0;
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrapPortable(Object o, boolean keepPortable, boolean cpy) {
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapPortable(key, keepPortable, cpy);

            Object val = entry.getValue();

            Object uVal = unwrapPortable(val, keepPortable, cpy);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }
        else if (o instanceof Collection)
            return unwrapPortablesIfNeeded((Collection<Object>)o, keepPortable, cpy);
        else if (o instanceof Map)
            return unwrapPortablesIfNeeded((Map<Object, Object>)o, keepPortable, cpy);
        else if (o instanceof CacheObject) {
            CacheObject co = (CacheObject)o;

            if (!keepPortable || co.isPlatformType())
                return unwrapPortable(co.value(this, true), keepPortable, cpy);
        }

        return o;
    }
}
