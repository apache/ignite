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

package org.apache.ignite.internal.processors.rest;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * REST protocol.
 */
public interface GridRestProtocol {
    /**
     * @return Protocol name.
     */
    public abstract String name();

    /**
     * Returns protocol properties for setting node attributes. Has meaningful result
     * only after protocol start.
     *
     * @return Protocol properties.
     */
    public abstract Collection<IgniteBiTuple<String, Object>> getProperties();

    /**
     * Starts protocol.
     *
     * @param hnd Command handler.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void start(GridRestProtocolHandler hnd) throws IgniteCheckedException;

    /**
     * Grid start callback.
     */
    public abstract void onKernalStart();

    /**
     * Stops protocol.
     */
    public abstract void stop();

    /**
     * Processor start callback.
     */
    void onProcessorStart();

    /**
     * Reloads the SSL context used by this protocol so that new connections use the updated certificates. Default
     * implementation is a no-op for protocols that do not support hot reload.
     *
     * @return {@code true} if SSL is enabled and the context was reloaded, {@code false} otherwise.
     * @throws IgniteCheckedException If the SSL context could not be reloaded.
     */
    public default boolean reloadSslContext() throws IgniteCheckedException {
        return false;
    }
}
