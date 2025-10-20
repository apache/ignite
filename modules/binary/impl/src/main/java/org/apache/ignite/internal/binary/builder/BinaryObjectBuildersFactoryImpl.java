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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.util.CommonUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Binary object builders factory implementation.
 * @see CommonUtils#loadService(Class)
 */
public class BinaryObjectBuildersFactoryImpl implements BinaryObjectBuildersFactory {
    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject obj) {
        return BinaryObjectBuilderImpl.wrap(obj);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryContext binaryCtx, String clsName) {
        return new BinaryObjectBuilderImpl(binaryCtx, clsName);
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
