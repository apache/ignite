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

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * External test for sorting.
 */
public class HadoopSortingExternalTest extends HadoopSortingTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String igniteInstanceName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(igniteInstanceName);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new JdkMarshaller());

        return cfg;
    }
}