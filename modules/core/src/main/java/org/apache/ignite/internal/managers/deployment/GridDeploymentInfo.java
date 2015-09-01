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

package org.apache.ignite.internal.managers.deployment;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Deployment info.
 */
public interface GridDeploymentInfo {
    /**
     * @return Class loader ID.
     */
    public IgniteUuid classLoaderId();

    /**
     * @return User version.
     */
    public String userVersion();

    /**
     * @return Sequence number.
     */
    public long sequenceNumber();

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deployMode();

    /**
     * @return Local deployment ownership flag.
     */
    public boolean localDeploymentOwner();

    /**
     * @return Participant map for SHARED mode.
     */
    public Map<UUID, IgniteUuid> participants();
}