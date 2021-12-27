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

package org.apache.ignite.client.handler;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client resource holder.
 */
public class ClientResource {
    /** Object. */
    private final Object obj;

    /** Release delegate. */
    private final @Nullable Runnable releaseRunnable;

    public ClientResource(@NotNull Object obj, @Nullable Runnable releaseRunnable) {
        this.obj = obj;
        this.releaseRunnable = releaseRunnable;
    }

    /**
     * Gets the underlying object.
     *
     * @param <T> Object type.
     * @return Object.
     */
    public <T> T get(Class<T> type) {
        if (!type.isInstance(obj)) {
            throw new IgniteException("Incorrect resource type. Expected " + type.getName() + ", but got " + obj.getClass().getName());
        }

        return (T) obj;
    }

    /**
     * Releases resources.
     */
    public void release() {
        if (releaseRunnable != null) {
            releaseRunnable.run();
        }
    }
}
