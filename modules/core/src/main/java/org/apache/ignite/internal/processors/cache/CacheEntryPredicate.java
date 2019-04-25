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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public interface CacheEntryPredicate extends IgnitePredicate<GridCacheEntryEx>, Message {
    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    /**
     * @param locked Entry locked
     */
    public void entryLocked(boolean locked);
}
