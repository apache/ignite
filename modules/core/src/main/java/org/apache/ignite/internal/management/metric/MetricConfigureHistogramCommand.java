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

package org.apache.ignite.internal.management.metric;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.visor.metric.VisorMetricTask;

import static org.apache.ignite.internal.management.api.CommandUtils.nodeOrNull;

/** */
public class MetricConfigureHistogramCommand implements ComputeCommand<MetricCommandArg, Map<String, ?>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Configure histogram metric";
    }

    /** {@inheritDoc} */
    @Override public Class<MetricConfigureHistogramCommandArg> argClass() {
        return MetricConfigureHistogramCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorMetricTask> taskClass() {
        return VisorMetricTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, GridClientNode> nodes, MetricCommandArg arg) {
        return nodeOrNull(arg.nodeId());
    }
}
