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

package org.apache.ignite.stream;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Convenience adapter to visit every key-value tuple in the stream. Note, that the visitor
 * does not update the cache. If the tuple needs to be stored in the cache,
 * then {@code cache.put(...)} should be called explicitely.
 */
public class StreamVisitor<K, V> implements StreamReceiver<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tuple visitor. */
    private IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> vis;

    /**
     * Visitor to visit every stream key-value tuple. Note, that the visitor
     * does not update the cache. If the tuple needs to be stored in the cache,
     * then {@code cache.put(...)} should be called explicitely.
     *
     * @param vis Stream key-value tuple visitor.
     */
    public StreamVisitor(IgniteBiInClosure<IgniteCache<K, V>, Map.Entry<K, V>> vis) {
        this.vis = vis;
    }

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        for (Map.Entry<K, V> entry : entries)
            vis.apply(cache, entry);
    }
}
