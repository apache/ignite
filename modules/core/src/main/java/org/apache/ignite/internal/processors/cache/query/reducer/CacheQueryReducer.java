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

package org.apache.ignite.internal.processors.cache.query.reducer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;

/**
 * This abstract class is base class for cache query reducers. They are responsible for reducing results of cache query.
 *
 * <T> is a type of cache query result item.
 */
public abstract class CacheQueryReducer<T> extends GridIteratorAdapter<T> {
    /** {@inheritDoc} */
    @Override public void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("CacheQueryReducer doesn't support removing items.");
    }
}
