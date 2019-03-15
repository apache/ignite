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

package org.apache.ignite.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CacheMetricCollector extends Collector {
    private Ignite ignite;

    public CacheMetricCollector(Ignite ignite) {
        this.ignite = ignite;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        // Cache metrics
        GaugeMetricFamily cacheMetrics = new GaugeMetricFamily("cache_metric", "help text", Arrays.asList("measure", "cache"));
        for (String c : ignite.cacheNames()) {
            CacheMetrics metrics = ignite.cache(c).localMetrics();
            cacheMetrics.addMetric(Arrays.asList("GetAverageTime",c), metrics.getAverageGetTime());
            cacheMetrics.addMetric(Arrays.asList("PutAverageTime",c), metrics.getAveragePutTime());
            cacheMetrics.addMetric(Arrays.asList("Size",c), metrics.getCacheSize());
        }

        // Node metrics
        ClusterMetrics igniteNodeMetrics = ignite.cluster().localNode().metrics();
        String nodeId = ignite.cluster().localNode().id().toString();
        GaugeMetricFamily nodeMetrics = new GaugeMetricFamily("node_metric", "help text", Arrays.asList("measure", "node"));
        nodeMetrics.addMetric(Arrays.asList("BusyTime", nodeId), igniteNodeMetrics.getBusyTimePercentage());
        nodeMetrics.addMetric(Arrays.asList("CurrentActiveJobs", nodeId), igniteNodeMetrics.getCurrentActiveJobs());

        // Bring it all together
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        mfs.add(cacheMetrics);
        mfs.add(nodeMetrics);
        return mfs;
    }
}
