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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.NotNull;

/**
 * Metrics registry.
 * Provide methods to register required metrics, gauges for Ignite internals.
 * Provide the way to obtain all registered metrics for exporters.
 */
public class MetricRegistry implements ReadOnlyMetricRegistry {
    /** Registered metrics. */
    private final ConcurrentHashMap<String, MetricGroup> groups = new ConcurrentHashMap<>();

    /** Metric group creation listeners. */
    private final List<Consumer<MetricGroup>> metricGrpCreationLsnrs = new CopyOnWriteArrayList<>();

    /** Logger. */
    private IgniteLogger log;

    /** For test usages only. */
    public MetricRegistry() {
        this(null);
    }

    /** @param log Logger. */
    public MetricRegistry(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Gets or creates metric group.
     *
     * @param name Group name.
     * @return Group of metrics.
     */
    public MetricGroup group(String name) {
        return groups.computeIfAbsent(name, n -> {
            MetricGroup mgrp = new MetricGroup(name, log);

            notifyListeners(mgrp, metricGrpCreationLsnrs);

            return mgrp;
        });
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<MetricGroup> iterator() {
        return groups.values().iterator();
    }

    /** {@inheritDoc} */
    @Override public void addMetricGroupCreationListener(Consumer<MetricGroup> lsnr) {
        metricGrpCreationLsnrs.add(lsnr);
    }

    /**
     * Removes group.
     *
     * @param grpName Group name.
     */
    public void remove(String grpName) {
        groups.remove(grpName);
    }

    /**
     * @param t Consumed object.
     * @param lsnrs Listeners.
     * @param <T> Type of consumed object.
     */
    private <T> void notifyListeners(T t, List<Consumer<T>> lsnrs) {
        for (Consumer<T> lsnr : lsnrs) {
            try {
                lsnr.accept(t);
            }
            catch (Exception e) {
                U.warn(log, "Metric listener error", e);
            }
        }
    }
}
