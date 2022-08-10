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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;

/** */
public class CacheJavaObjectIndexKey extends JavaObjectIndexKey {
    /** */
    private final CacheObject obj;

    /** Object value context. */
    private final CacheObjectValueContext valCtx;

    /** */
    private byte[] serialized;

    /** */
    private Object key;

    /**
     * Constructor.
     *
     * @param obj Object.
     * @param valCtx Object value context.
     */
    public CacheJavaObjectIndexKey(CacheObject obj, CacheObjectValueContext valCtx) {
        assert obj != null;

        if (obj instanceof BinaryObjectImpl) {
            ((BinaryObjectImpl)obj).detachAllowed(true);
            obj = ((BinaryObjectImpl)obj).detach();
        }

        this.obj = obj;
        this.valCtx = valCtx;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        if (key == null)
            key = obj.isPlatformType() ? obj.value(valCtx, false) : obj;

        return key;
    }

    /** {@inheritDoc} */
    @Override public byte[] bytesNoCopy() {
        try {
            if (serialized == null) {
                // Result must be the same as `marshaller.marshall(obj.value(coctx, false));`
                if (obj.cacheObjectType() == CacheObject.TYPE_REGULAR)
                    return obj.valueBytes(valCtx);

                // For user-provided and array types.
                serialized = IndexProcessor.serializer.serialize(obj);
            }

            return serialized;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
