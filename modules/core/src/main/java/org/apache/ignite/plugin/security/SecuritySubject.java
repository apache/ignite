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

package org.apache.ignite.plugin.security;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Security subject representing authenticated node with a set of permissions.
 */
public interface SecuritySubject extends Serializable {
    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    public UUID id();

    /**
     * Gets subject type for node.
     *
     * @return Subject type.
     */
    public SecuritySubjectType type();

    /**
     * Login provided via subject security credentials.
     *
     * @return Login object.
     */
    public Object login();

    /**
     * Gets subject connection address. Usually {@link InetSocketAddress} representing connection IP and port.
     *
     * @return Subject connection address.
     */
    public InetSocketAddress address();

    /**
     * Authorized permission set for the subject.
     *
     * @return Authorized permission set for the subject.
     */
    public SecurityPermissionSet permissions();
}
