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

package org.apache.ignite.spi.metric;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.spi.metric.log.LogExporterSpi;

/**
 * Exporter of metric information to the external recepient.
 * Expected, that each implementation would support some specific protocol.
 *
 * Implementation of this Spi should work by push paradigm.
 * So, after start, SPI should export metric info into recepient on each {@link #export()} call.
 * {@link LogExporterSpi} that prints metric info into {@link IgniteLogger} is example of expected behaviour.
 *
 * @see MetricRegistry
 * @see Metric
 * @see BooleanMetric
 * @see DoubleMetric
 * @see IntMetric
 * @see LongMetric
 * @see ObjectMetric
 * @see LogExporterSpi
 * @see MetricExporterSpi
 */
public interface MetricExporterPushSpi extends MetricExporterSpi {
    /**
     * @return Period in milliseconds after {@link #export()} method should be called.
     */
    public long getPeriod();

    /** Sets period in milliseconds after {@link #export()} method should be called. */
    public void setPeriod(long period);

    /**
     * Callback to do the export of metrics info.
     * Method will be called into some Ignite managed thread each {@link #timeout()} millisecond.
     */
    public void export();
}
