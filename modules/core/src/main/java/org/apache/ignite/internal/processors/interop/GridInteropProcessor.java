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

package org.apache.ignite.internal.processors.interop;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.*;
import org.jetbrains.annotations.*;

/**
 * Interop processor.
 */
public interface GridInteropProcessor extends GridProcessor {
    /**
     * Release start latch.
     */
    public void releaseStart();

    /**
     * Await start on native side.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void awaitStart() throws IgniteCheckedException;

    /**
     * @return Environment pointer.
     */
    public long environmentPointer() throws IgniteCheckedException;

    /**
     * @return Grid name.
     */
    public String gridName();

    /**
     * Gets native wrapper for default Grid projection.
     *
     * @return Native compute wrapper.
     * @throws IgniteCheckedException If failed.
     */
    public GridInteropTarget projection() throws IgniteCheckedException;

    /**
     * Gets native wrapper for cache with the given name.
     *
     * @param name Cache name ({@code null} for default cache).
     * @return Native cache wrapper.
     * @throws IgniteCheckedException If failed.
     */
    public GridInteropTarget cache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Gets native wrapper for data loader for cache with the given name.
     *
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Native data loader wrapper.
     * @throws IgniteCheckedException If failed.
     */
    public GridInteropTarget dataLoader(@Nullable String cacheName) throws IgniteCheckedException;

    /**
     * Stops grid.
     *
     * @param cancel Cancel flag.
     */
    public void close(boolean cancel);
}
