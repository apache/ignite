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

package org.apache.ignite.internal.processors.metric;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * Standalone metric manager.
 */
public class MetricManager implements ReadOnlyMetricManager {
    /** Registered metrics registries. */
    private final ConcurrentHashMap<String, ReadOnlyMetricRegistry> registries = new ConcurrentHashMap<>();

    /** Metric registry creation listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegCreationLsnrs = new CopyOnWriteArrayList<>();

    /** Metric registry remove listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegRemoveLsnrs = new CopyOnWriteArrayList<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final Function<String, Long> hitRateCfgProvider;

    /** */
    private final Function<String, long[]> histogramCfgProvider;

    /**
     * @param log Logger.
     * @param histogramCfgProvider Historgram config provider.
     * @param hitRateCfgProvider HitRate config provider.
     */
    public MetricManager(
        MetricExporterSpi[] spis,
        Function<String, Long> hitRateCfgProvider,
        Function<String, long[]> histogramCfgProvider,
        IgniteLogger log
    ) {
        this.histogramCfgProvider = histogramCfgProvider;
        this.hitRateCfgProvider = hitRateCfgProvider;
        this.log = log;
    }

    /**
     * Gets or creates metric registry.
     *
     * @param name Group name.
     * @return Group of metrics.
     */
    public MetricRegistry registry(String name) {
        return (MetricRegistry)registries.computeIfAbsent(name, n -> {
            MetricRegistry mreg = new MetricRegistry(name, hitRateCfgProvider, histogramCfgProvider, log);

            notifyListeners(mreg, metricRegCreationLsnrs, log);

            return mreg;
        });
    }

    /** */
    MetricRegistry get(String name) {
        return (MetricRegistry)registries.get(name);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<ReadOnlyMetricRegistry> iterator() {
        return registries.values().iterator();
    }

    /** {@inheritDoc} */
    @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
        metricRegCreationLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void addMetricRegistryRemoveListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
        metricRegRemoveLsnrs.add(lsnr);
    }

    /**
     * Removes metric registry.
     *
     * @param regName Metric registry name.
     */
    public void remove(String regName) {
        registries.computeIfPresent(regName, (key, mreg) -> {
            notifyListeners(mreg, metricRegRemoveLsnrs, log);

            return null;
        });
    }
}
