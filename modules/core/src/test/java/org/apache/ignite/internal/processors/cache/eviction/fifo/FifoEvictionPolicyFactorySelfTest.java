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

package org.apache.ignite.internal.processors.cache.eviction.fifo;

import javax.cache.configuration.Factory;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.internal.processors.cache.eviction.EvictionAbstractTest;

/**
 * FIFO eviction policy tests.
 */
public class FifoEvictionPolicyFactorySelfTest extends FifoEvictionPolicySelfTest {
       /** {@inheritDoc} */
    @Override protected Factory<FifoEvictionPolicy<String, String>> getPolicyFactory() {
        final int batchSize = this.plcBatchSize;
        final int plcMax = this.plcMax;
        final long plcMaxMemSize = this.plcMaxMemSize;

        return new Factory<FifoEvictionPolicy<String, String>>(){
            /** {@inheritDoc} */
            @Override public FifoEvictionPolicy<String, String> create() {
                FifoEvictionPolicy<String, String> plc = new FifoEvictionPolicy<>();

                plc.setMaxSize(plcMax);
                plc.setBatchSize(batchSize);
                plc.setMaxMemorySize(plcMaxMemSize);

                return plc;
            }
        };
    }
}