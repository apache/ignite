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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Closure of cluster wide update of distributed property.
 */
@FunctionalInterface
public interface PropertyUpdateClosure {

    /**
     * Update property on cluster.
     *
     * @param key Property key.
     * @param newValue New value.
     * @return Future this boolean value.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> update(String key, Serializable newValue) throws IgniteCheckedException;
}
