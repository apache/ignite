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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Arrays;
import java.util.List;

/**
 * Test dynamic schema operations from server node which do not pass node filter and which is coordinator.
 */
public class DynamicIndexBasicServerNodeFIlterCoordinatorSelfTest extends DynamicIndexBasicAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(IDX_SRV_CRD, true),
            serverConfiguration(IDX_SRV_NON_CRD),
            clientConfiguration(IDX_CLI),
            serverConfiguration(IDX_SRV_FILTERED, true)
        );
    }

    /** {@inheritDoc} */
    @Override protected int nodeIndex() {
        return IDX_SRV_CRD;
    }
}
