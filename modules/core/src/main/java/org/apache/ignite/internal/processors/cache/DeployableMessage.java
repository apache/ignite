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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

/**
 * A {@link GridCacheMessage} with custom deployment logic that cannot be inferred from field types (conditional
 * deployment, non-standard accessors, etc.). The generated {@code *Deployer} calls {@link #deploy} after
 * its inferred field deployment, mirroring how a generated marshaller calls {@code MarshallableMessage#marshal}.
 */
public interface DeployableMessage {
    /**
     * Prepares deployment info for fields whose handling cannot be inferred from their type.
     *
     * @param ctx Cache shared context.
     * @throws IgniteCheckedException If failed.
     */
    void deploy(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException;
}
