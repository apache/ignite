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

package org.apache.ignite.internal.binary.builder;

import java.util.Iterator;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to provide static methods to create {@link BinaryObjectBuilder}.
 */
public class BinaryObjectBuilders {
    /**
     * @param obj Object to convert to builder.
     * @return Builder instance.
     */
    public static BinaryObjectBuilder builder(BinaryObject obj) {
        return BinaryObjectBuilderImpl.wrap(obj);
    }

    /**
     * @param binaryCtx Binary context.
     * @param clsName Class name.
     * @return Builder instance.
     */
    public static BinaryObjectBuilder builder(BinaryContext binaryCtx, String clsName) {
        return new BinaryObjectBuilderImpl(binaryCtx, clsName);
    }

    /**
     * Prepare affinity field for builder (if possible).
     *
     * @param builder Builder.
     */
    public static void prepareAffinityField(BinaryObjectBuilder builder, CacheObjectContext cacheObjCtx) {
        if (cacheObjCtx.customAffinityMapper())
            return;

        assert builder instanceof BinaryObjectBuilderImpl;

        BinaryObjectBuilderImpl builder0 = (BinaryObjectBuilderImpl)builder;

        CacheDefaultBinaryAffinityKeyMapper mapper =
            (CacheDefaultBinaryAffinityKeyMapper)cacheObjCtx.defaultAffMapper();

        BinaryField field = mapper.affinityKeyField(builder0.typeId());

        if (field != null) {
            String fieldName = field.name();

            builder0.affinityFieldName(fieldName);
        }
    }

    /**
     * @param obj Value to unwrap.
     * @return Unwrapped value.
     */
    static Object unwrapLazy(@Nullable Object obj) {
        if (obj instanceof BinaryLazyValue)
            return ((BinaryLazyValue)obj).value();

        return obj;
    }

    /**
     * @param delegate Iterator to delegate.
     * @return New iterator.
     */
    static Iterator<Object> unwrapLazyIterator(final Iterator<Object> delegate) {
        return new Iterator<Object>() {
            @Override public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override public Object next() {
                return unwrapLazy(delegate.next());
            }

            @Override public void remove() {
                delegate.remove();
            }
        };
    }
}
