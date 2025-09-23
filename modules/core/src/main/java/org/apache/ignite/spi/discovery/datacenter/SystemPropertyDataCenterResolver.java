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

package org.apache.ignite.spi.discovery.datacenter;

import org.apache.ignite.IgniteSystemProperties;

import static org.apache.ignite.IgniteCommonsSystemProperties.getString;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;

/**
 * Implementation of {@link DataCenterResolver} interface that uses an {@link IgniteSystemProperties#IGNITE_DATA_CENTER_ID}
 * system property to obtain Data Center ID value.
 */
public class SystemPropertyDataCenterResolver implements DataCenterResolver {
    /** {@inheritDoc} */
    @Override public String resolveDataCenterId() {
        return getString(IGNITE_DATA_CENTER_ID);
    }
}
