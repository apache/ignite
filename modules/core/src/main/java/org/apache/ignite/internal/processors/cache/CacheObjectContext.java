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

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cacheobject.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class CacheObjectContext implements Externalizable {
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

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public CacheObjectContext() {
        // No-op.
    }

    /**
     * @param kernalCtx Kernal context.
     * @param dfltAffMapper Default affinity mapper.
     * @param cpyOnGet Copy on get flag.
     * @param storeVal {@code True} if should store unmarshalled value in cache.
     */
    public CacheObjectContext(GridKernalContext kernalCtx,
        AffinityKeyMapper dfltAffMapper,
        boolean cpyOnGet,
        boolean storeVal
    ) {
        this.kernalCtx = kernalCtx;
        this.dfltAffMapper = dfltAffMapper;
        this.cpyOnGet = cpyOnGet;
        this.storeVal = storeVal;

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
     * Unwraps object if needed.
     *
     * @param o Object to unwrap.
     * @param keepPortable Keep portable flag. Used for portable objects only. Ignored in other cases.
     * @return Unwrapped object.
     */
    public Object unwrapIfNeeded(Object o, boolean keepPortable) {
        if (o == null)
            return null;

        if (!kernalContext().cacheObjects().isFieldsIndexingEnabled())
            return o;

        if (keepPortable || !kernalContext().cacheObjects().isIndexedObjectOrCollectionType(o.getClass()))
            return o;

        return unwrapObject(o);
    }

    /**
     * Unwraps collection if needed.
     *
     * @param col Collection to unwrap.
     * @param keepPortable Keep portable flag. Used for portable objects only. Ignored in other cases.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapIfNeeded(Collection<Object> col, boolean keepPortable) {
        if (F.isEmpty(col))
            return col;

        if (keepPortable || !kernalContext().cacheObjects().isFieldsIndexingEnabled())
            return col;

        return (Collection<Object>)unwrapObject(col);
    }

    /**
     * Unwraps object if needed.
     *
     * @param obj Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrapObject(Object obj) {
        IgniteCacheObjectProcessor objProc = kernalContext().cacheObjects();

        if (objProc.isIndexedObject(obj))
            return objProc.unwrapIndexedObject(obj);
        else if (obj instanceof Map.Entry) {
            Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>)obj;

            Object key = entry.getKey();

            boolean unwrapped = false;

            if (objProc.isIndexedObject(key)) {
                key = objProc.unwrapIndexedObject(key);

                unwrapped = true;
            }

            Object val = entry.getValue();

            if (objProc.isIndexedObject(val)) {
                val = objProc.unwrapIndexedObject(val);

                unwrapped = true;
            }

            return unwrapped ? F.t(key, val) : obj;
        }
        else if (obj instanceof Collection) {
            Collection<Object> col = (Collection<Object>)obj;

            if (col instanceof ArrayList) {
                List<Object> list = (List<Object>)col;

                int size = list.size();

                for (int i = 0; i < size; i++) {
                    Object old = list.get(i);

                    Object unwrapped = unwrapObject(old);

                    if (old != unwrapped)
                        list.set(i, unwrapped);
                }

                return list;
            }
            else if (col instanceof Set) {
                Collection<Object> set = new HashSet<>();

                for (Object obj0 : col)
                    set.add(unwrapObject(obj0));
            }
            else {
                Collection<Object> col0 = new ArrayList<>(col.size());

                for (Object obj0 : col)
                    col0.add(unwrapObject(obj0));

                return col0;
            }
        }
        else if (obj instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>)obj;

            Map<Object, Object> map0 = U.newHashMap(map.size());

            for (Map.Entry<Object, Object> e : map.entrySet())
                map0.put(unwrapObject(e.getKey()), unwrapObject(e.getValue()));
        }

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false; // TODO IGNITE-950
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false; // TODO IGNITE-950
    }
}
