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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;

/** Message serialization logic. */
public interface MessageSerializer<M extends Message> {
    /**
     * Writes this message to provided byte buffer.
     *
     * @param msg Message instance.
     * @param writer Writer.
     * @return Whether message was fully written.
     */
    public boolean writeTo(M msg, MessageWriter writer);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param msg Message instance.
     * @param reader Reader.
     * @return Whether message was fully read.
     */
    public boolean readFrom(M msg, MessageReader reader);

    /**
     * Marshalls cache-object fields on the user thread.
     *
     * @param msg Message instance.
     * @param kctx Kernal context.
     * @param nested Nested context.
     * @throws IgniteCheckedException If marshalling fails.
     */
    public default void prepareMarshal(M msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {
        // No-op by default.
    }

    /**
     * Unmarshalls cache-object fields on the user thread.
     *
     * @param msg Message instance.
     * @param kctx Kernal context.
     * @param nested Nested context.
     * @param clsLdr Classloader.
     * @throws IgniteCheckedException If unmarshalling fails.
     */
    public default void finishUnmarshal(M msg, GridKernalContext kctx, GridCacheContext<?, ?> nested, ClassLoader clsLdr) 
        throws IgniteCheckedException {
        // No-op by default.
    }

    /**
     * Unmarshalls message fields.
     * 
     * @param msg Message instance.
     * @param kctx Kernal context.
     * @throws IgniteCheckedException If unmarshalling fails.
     */
    public default void finishUnmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        // No-op by default.
    }
}
