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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteCheckedException;

/**
 * Closure that internally applies given {@link SchemaIndexCacheVisitorClosure} to some set of entries.
 */
public interface SchemaIndexCacheVisitor {
    /**
     * Visit cache entries and pass them to closure.
     *
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException;
}
