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

package org.apache.ignite.spi.metric.list.view;

import java.util.UUID;
import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.list.SystemView;
import org.apache.ignite.spi.metric.list.SystemViewRow;

/**
 * Service representation for a {@link SystemView}.
 */
public interface ServiceView extends SystemViewRow {
    /** @return Service name. */
    @Order(1)
    public String name();

    /** @return Service id. */
    @Order
    public IgniteUuid serviceId();

    /** @return Service class. */
    @Order(2)
    public Class<?> serviceClass();

    /** @return Total count of service instances. */
    @Order(5)
    public int totalCount();

    /** @return Maximum instance count per node. */
    @Order(6)
    public int maxPerNodeCount();

    /** @return Cache name. */
    @Order(3)
    public String cacheName();

    /** @return Affininty key value. */
    public String affinityKeyValue();

    /** @return Node filter. */
    public Class<?> nodeFilter();

    /** @return {@code True} if statically configured. */
    public boolean staticallyConfigured();

    /** @return Origin node id. */
    @Order(4)
    public UUID originNodeId();
}
