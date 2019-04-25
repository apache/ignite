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
}