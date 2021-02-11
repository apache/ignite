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

package org.apache.ignite.client;

/**
 * Thin client services facade.
 */
public interface ClientServices {
    /**
     * Gets the cluster group to which this {@code ClientServices} instance belongs.
     *
     * @return Cluster group to which this {@code ClientServices} instance belongs.
     */
    public ClientClusterGroup clusterGroup();

    /**
     * Gets a remote handle on the service.
     * <p>
     * Note: There are no guarantees that each method invocation for the same proxy will always contact the same remote
     * service (on the same remote node).
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @return Proxy over remote service.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf);

    /**
     * Gets a remote handle on the service with timeout.
     * <p>
     * Note: There are no guarantees that each method invocation for the same proxy will always contact the same remote
     * service (on the same remote node).
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @param timeout If greater than 0 created proxy will wait for service availability only specified time,
     *  and will limit remote service invocation time.
     * @return Proxy over remote service.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf, long timeout);
}
