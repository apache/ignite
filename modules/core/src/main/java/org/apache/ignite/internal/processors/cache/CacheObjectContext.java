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
import org.apache.ignite.internal.binary.BinaryUtils;
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

    /**
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped object.
     */
    public Object unwrapBinaryIfNeeded(Object o, boolean keepBinary) {
        return unwrapBinaryIfNeeded(o, keepBinary, true);
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

        return unwrapBinary(o, keepBinary, cpy);
    }

    /**
     * @param col Collection of objects to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapBinariesIfNeeded(Collection<Object> col, boolean keepBinary) {
        return unwrapBinariesIfNeeded(col, keepBinary, true);
    }

    /**
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapBinariesIfNeeded(Collection<Object> col, boolean keepBinary, boolean cpy) {
        if (col instanceof ArrayList)
            return unwrapBinaries((ArrayList<Object>)col, keepBinary, cpy);

        if (col instanceof Set)
            return unwrapBinaries((Set<Object>)col, keepBinary, cpy);

        Collection<Object> col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapBinary(obj, keepBinary, cpy));

        return col0;
    }

    /**
     * Unwrap array of binaries if needed.
     *
     * @param arr Array.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy.
     * @return Result.
     */
    public Object[] unwrapBinariesInArrayIfNeeded(Object[] arr, boolean keepBinary, boolean cpy) {
        Object[] res = new Object[arr.length];

        for (int i = 0; i < arr.length; i++)
            res[i] = unwrapBinary(arr[i], keepBinary, cpy);

        return res;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    private Map<Object, Object> unwrapBinariesIfNeeded(Map<Object, Object> map, boolean keepBinary, boolean cpy) {
        if (keepBinary)
            return map;

        Map<Object, Object> map0 = BinaryUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapBinary(e.getKey(), keepBinary, cpy), unwrapBinary(e.getValue(), keepBinary, cpy));

        return map0;
    }

    /**
     * Unwraps array list.
     *
     * @param col List to unwrap.
     * @return Unwrapped list.
     */
    private Collection<Object> unwrapBinaries(ArrayList<Object> col, boolean keepBinary, boolean cpy) {
        int size = col.size();

        col = new ArrayList<>(col);

        for (int i = 0; i < size; i++) {
            Object o = col.get(i);

            Object unwrapped = unwrapBinary(o, keepBinary, cpy);

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
    private Set<Object> unwrapBinaries(Set<Object> set, boolean keepBinary, boolean cpy) {
        Set<Object> set0 = BinaryUtils.newSet(set);

        for (Object obj : set)
            set0.add(unwrapBinary(obj, keepBinary, cpy));

        return set0;
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrapBinary(Object o, boolean keepBinary, boolean cpy) {
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(key, keepBinary, cpy);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(val, keepBinary, cpy);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }
        else if (o instanceof Collection)
            return unwrapBinariesIfNeeded((Collection<Object>)o, keepBinary, cpy);
        else if (o instanceof Map)
            return unwrapBinariesIfNeeded((Map<Object, Object>)o, keepBinary, cpy);
        else if (o instanceof Object[])
            return unwrapBinariesInArrayIfNeeded((Object[])o, keepBinary, cpy);
        else if (o instanceof CacheObject) {
            CacheObject co = (CacheObject)o;

            if (!keepBinary || co.isPlatformType())
                return unwrapBinary(co.value(this, cpy), keepBinary, cpy);
        }

        return o;
    }
}
