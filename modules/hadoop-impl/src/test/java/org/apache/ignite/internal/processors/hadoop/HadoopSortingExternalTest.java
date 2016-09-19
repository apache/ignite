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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * External test for sorting.
 */
public class HadoopSortingExternalTest extends HadoopSortingTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new JdkMarshaller());

        return cfg;
    }
}