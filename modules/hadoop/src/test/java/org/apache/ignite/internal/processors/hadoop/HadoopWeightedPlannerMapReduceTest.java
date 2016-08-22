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
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopWeightedMapReducePlanner;

/**
 * Tests whole map-red execution Weighted planner.
 */
public class HadoopWeightedPlannerMapReduceTest extends HadoopMapReduceTest {
    /** {@inheritDoc} */
    @Override protected HadoopConfiguration createHadoopConfiguration() {
        HadoopConfiguration hadoopCfg = new HadoopConfiguration();

        // Use weighted planner with default settings:
        IgniteHadoopWeightedMapReducePlanner planner = new IgniteHadoopWeightedMapReducePlanner();

        hadoopCfg.setMapReducePlanner(planner);

        return hadoopCfg;
    }
}
