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

package org.apache.ignite.internal.processors.hadoop.impl.client;

import org.apache.ignite.configuration.HadoopConfiguration;

/**
 * Hadoop client protocol tests in embedded process mode.
 */
public class HadoopClientProtocolEmbeddedSelfTest extends HadoopClientProtocolSelfTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String igniteInstanceName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(igniteInstanceName);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(false);

        return cfg;
    }
}