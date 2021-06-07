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

package org.apache.ignite.app;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for handling grid lifecycle.
 */
@SuppressWarnings("UnnecessaryInterfaceModifier")
public interface Ignition {
    /**
     * Starts Ignite node with optional bootstrap configuration in json format.
     *
     * @param name Name of the node. Couldn't be {@code null}.
     * @param jsonStrBootstrapCfg Node configuration in json format. Could be {@code null}.
     * @return Started Ignite node.
     */
    // TODO: IGNITE-14599 Add generic way to bootstrap configuration.
    public Ignite start(@NotNull String name, @Nullable String jsonStrBootstrapCfg);
}
