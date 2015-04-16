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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.*;

/**
 * {@link CacheQueries} implementation.
 */
public class CacheQueriesImpl<K, V> implements CacheQueries<K, V> {
    /** */
    private GridCacheContext<K, V> ctx;

    /** */
    private boolean keepPortable;

    /**
     * @param ctx Context.
     * @param keepPortable Keep portable flag.
     */
    public CacheQueriesImpl(GridCacheContext<K, V> ctx, boolean keepPortable) {
        assert ctx != null;

        this.ctx = ctx;
        this.keepPortable = keepPortable;
    }
}
