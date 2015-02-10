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

import org.apache.ignite.internal.processors.cache.serialization.*;

import java.util.*;

/**
 * Cache manager responsible for translating user objects into cache objects.
 */
public class IgniteCacheOsSerializationManager<K, V> extends GridCacheManagerAdapter<K, V>
    implements IgniteCacheSerializationManager<K, V> {
    /** {@inheritDoc} */
    @Override public boolean portableEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean keepPortableInStore() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapPortableIfNeeded(Object o, boolean keepPortable) {
        return o;
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        return col;
    }
}
