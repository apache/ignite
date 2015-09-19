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

package org.apache.ignite.internal.processors.cache.portable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.portable.api.PortableObject;

/**
 *
 */
public class CacheObjectPortableContext extends CacheObjectContext {
    /** */
    private boolean portableEnabled;

    /**
     * @param kernalCtx Kernal context.
     * @param portableEnabled Portable enabled flag.
     * @param cpyOnGet Copy on get flag.
     * @param storeVal {@code True} if should store unmarshalled value in cache.
     */
    public CacheObjectPortableContext(GridKernalContext kernalCtx,
        boolean cpyOnGet,
        boolean storeVal,
        boolean portableEnabled) {
        super(kernalCtx, portableEnabled ? new CacheDefaultPortableAffinityKeyMapper() :
            new GridCacheDefaultAffinityKeyMapper(), cpyOnGet, storeVal);

        this.portableEnabled = portableEnabled;
    }

    /**
     * @return Portable enabled flag.
     */
    public boolean portableEnabled() {
        return portableEnabled;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapPortableIfNeeded(Object o, boolean keepPortable) {
        if (o == null)
            return null;

        if (keepPortable || !portableEnabled() || !PortableUtils.isPortableOrCollectionType(o.getClass()))
            return o;

        return unwrapPortable(o);
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        if (keepPortable || !portableEnabled())
            return col;

        if (col instanceof ArrayList)
            return unwrapPortables((ArrayList<Object>)col);

        if (col instanceof Set)
            return unwrapPortables((Set<Object>)col);

        Collection<Object> col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapPortable(obj));

        return col0;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    public Map<Object, Object> unwrapPortablesIfNeeded(Map<Object, Object> map, boolean keepPortable) {
        if (keepPortable || !portableEnabled())
            return map;

        Map<Object, Object> map0 = PortableUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapPortable(e.getKey()), unwrapPortable(e.getValue()));

        return map0;
    }

    /**
     * Unwraps array list.
     *
     * @param col List to unwrap.
     * @return Unwrapped list.
     */
    private Collection<Object> unwrapPortables(ArrayList<Object> col) {
        int size = col.size();

        for (int i = 0; i < size; i++) {
            Object o = col.get(i);

            Object unwrapped = unwrapPortable(o);

            if (o != unwrapped)
                col.set(i, unwrapped);
        }

        return col;
    }

    /**
     * Unwraps set with portables.
     *
     * @param set Set to unwrap.
     * @return Unwrapped set.
     */
    private Set<Object> unwrapPortables(Set<Object> set) {
        Set<Object> set0 = PortableUtils.newSet(set);

        Iterator<Object> iter = set.iterator();

        while (iter.hasNext())
            set0.add(unwrapPortable(iter.next()));

        return set0;
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrapPortable(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            boolean unwrapped = false;

            if (key instanceof PortableObject) {
                key = ((PortableObject)key).deserialize();

                unwrapped = true;
            }

            Object val = entry.getValue();

            if (val instanceof PortableObject) {
                val = ((PortableObject)val).deserialize();

                unwrapped = true;
            }

            return unwrapped ? F.t(key, val) : o;
        }
        else if (o instanceof Collection)
            return unwrapPortablesIfNeeded((Collection<Object>)o, false);
        else if (o instanceof Map)
            return unwrapPortablesIfNeeded((Map<Object, Object>)o, false);
        else if (o instanceof PortableObject)
            return ((PortableObject)o).deserialize();

        return o;
    }
}
