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
import org.apache.ignite.spi.metric.list.MonitoringRow;

/** */
public interface ServiceView extends MonitoringRow<IgniteUuid> {
    /** {@inheritDoc} */
    @Override default IgniteUuid monitoringRowId() {
        return serviceId();
    }

    /** */
    @Order(1)
    public String name();

    /** */
    @Order
    public IgniteUuid serviceId();

    /** */
    @Order(2)
    public Class<?> serviceClass();

    /** */
    @Order(5)
    public int totalCount();

    /** */
    @Order(6)
    public int maxPerNodeCount();

    /** */
    @Order(3)
    public String cacheName();

    /** */
    public String affinityKeyValue();

    /** */
    public Class<?> nodeFilter();

    /** */
    public boolean staticallyConfigured();

    /** */
    @Order(4)
    public UUID originNodeId();
}
