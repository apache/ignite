/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cacheobject;

import java.util.Arrays;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps value provided by user, must be copied before stored in cache.
 */
public class UserCacheObjectByteArrayImpl extends CacheObjectByteArrayImpl {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public UserCacheObjectByteArrayImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public UserCacheObjectByteArrayImpl(byte[] val) {
        super(val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
        return super.value(ctx, false); // Do not need copy since user value is not in cache.
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        byte[] valCpy = Arrays.copyOf(val, val.length);

        return new CacheObjectByteArrayImpl(valCpy);
    }
}
