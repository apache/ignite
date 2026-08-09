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

package org.apache.ignite.internal;

import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Implemented by messages that carry classes deployed from another node. The deployment lets the generated marshaller
 * resolve the class loader those classes are read with, the same way {@code CacheIdAware} lets it resolve the cache
 * object context.
 * <p>
 * Resolving may have to request the deployment from its owner and block, and it fails when the classes are gone, so a
 * message stating this must be unmarshalled where blocking is allowed and where the failure reaches whoever waits for
 * it. A message read on a socket-reading thread promises neither: a discovery custom message, for one, is a nested
 * field of its envelope, so the envelope's marshaller reads the whole tree there, and a missing class is swallowed
 * with a warning. Such a message keeps its deployment as a plain field and asks {@code GridDeploymentManager} for the
 * loader where it is read, as {@code StartRequestData} does.
 *
 * @see MarshallableMessage
 */
public interface DeploymentAware extends Message {
    /** @return Deployment of the classes the message carries. */
    public GridDeploymentInfo deploymentInfo();

    /** @return Name of a class the deployment must be able to load. */
    public String deployedClassName();
}
