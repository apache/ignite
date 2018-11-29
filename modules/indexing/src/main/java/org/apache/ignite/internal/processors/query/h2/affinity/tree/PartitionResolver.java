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

package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Partition resolver.
 */
public class PartitionResolver {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache name. */
    private final String cacheName;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     */
    public PartitionResolver(GridKernalContext ctx, String cacheName) {
        this.ctx = ctx;
        this.cacheName = cacheName;
    }

    /**
     * Resolve partition for the given object.
     *
     * @param obj Object.
     * @return Partition.
     */
    public int resolve(Object obj) {
        try {
            return ctx.affinity().partition(cacheName, obj);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to resolve partition [cacheName=" + cacheName + ']', e);
        }
    }
}
