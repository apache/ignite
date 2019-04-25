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

package org.apache.ignite.internal.processors.cache.persistence.tree.util;

import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;

/**
 * Wrapper factory for {@link PageHandler} instances.
 *
 * @param <R> Result type of actual {@link PageHandler} class.
 */
public interface PageHandlerWrapper<R> {
    /**
     * Wraps given {@code hnd}.
     *
     * @param tree Instance of {@link BPlusTree} where given {@code} is used.
     * @param hnd Page handler to wrap.
     * @return Wrapped version of given {@code hnd}.
     */
    public PageHandler<?, R> wrap(BPlusTree<?, ?> tree, PageHandler<?, R> hnd);
}
