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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Message which needs to be marshalled and unmarshalled before sending or processing it.
 */
public interface GridCacheQueryMarshallable {
    /**
     * @param m Marshaller.
     */
    public void marshall(Marshaller m);

    /**
     * @param m Marshaller.
     * @param ctx Context.
     */
    public void unmarshall(Marshaller m, GridKernalContext ctx);
}
