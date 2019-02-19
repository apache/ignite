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

package org.apache.ignite.internal.processors.cache.datastructures.replicated;

import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.util.AttributeNodeFilter;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Tests IgniteSet with node filter on {@code REPLICATED} cache.
 */
public class GridCacheReplicatedSetWithNodeFilterSelfTest extends GridCacheReplicatedSetSelfTest {
    /** {@inheritDoc} */
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration cfg = super.collectionConfiguration();

        cfg.setNodeFilter(new AttributeNodeFilter(ATTR_IGNITE_INSTANCE_NAME, getTestIgniteInstanceName(0)));

        return cfg;
    }
}
