/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which outputs measurements to a {@link PrintStream}, like {@code System.out}.
 *
 * Fork form {@link com.codahale.metrics.ConsoleReporter}
 */
public class MetricReporter {

    /**
     * Returns a new {@link Builder} for {@link MetricReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link MetricReporter}
     */
    public static Builder forRegistry(final MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * Report the current values of all metrics in the registry.
     */
    public void report() {
        synchronized (this) {
            report(this.registry.getGauges(this.filter), //
                this.registry.getCounters(this.filter), //
                this.registry.getHistograms(this.filter), //
                this.registry.getMeters(this.filter), //
                this.registry.getTimers(this.filter));
        }
    }

    /**
     * A builder for {@link MetricReporter} instances. Defaults to using the default locale and time zone, writing to
     * {@code System.out}, converting rates to events/second, converting durations to milliseconds, and not filtering
     * metrics.
     */
    public static class Builder {

        private final MetricRegistry registry;

        private String prefix;
        private PrintStream output;
        private Locale locale;
        private Clock clock;
        private TimeZone timeZone;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Set<MetricAttribute> disabledMetricAttributes;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.prefix = "";
            this.output = System.out;
            this.locale = Locale.getDefault();
            this.clock = Clock.defaultClock();
            this.timeZone = TimeZone.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.disabledMetricAttributes = Collections.emptySet();
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all banner names
         * @return {@code this}
         */
        public Builder prefixedWith(final String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Write to the given {@link PrintStream}.
         *
         * @param output a {@link PrintStream} instance.
         * @return {@code this}
         */
        public Builder outputTo(final PrintStream output) {
            this.output = output;
            return this;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formattedFor(final Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(final Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link TimeZone} for the time.
         *
         * @param timeZone a {@link TimeZone}
         * @return {@code this}
         */
        public Builder formattedFor(final TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(final TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(final TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(final MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15"). See {@link
         * MetricAttribute}.
         *
         * @param disabledMetricAttributes a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(final Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        /**
         * Builds a {@link MetricReporter} with the given properties.
         *
         * @return a {@link MetricReporter}
         */
        public MetricReporter build() {
            return new MetricReporter(this.registry, //
                this.output, //
                this.prefix, //
                this.locale, //
                this.clock, //
                this.timeZone, //
                this.rateUnit, //
                this.durationUnit, //
                this.filter, //
                this.disabledMetricAttributes);
        }
    }

    private static final int CONSOLE_WIDTH = 80;

    private final MetricRegistry registry;
    private final Set<MetricAttribute> disabledMetricAttributes;
    private final MetricFilter filter;
    private final long durationFactor;
    private final String durationUnit;
    private final long rateFactor;
    private final String rateUnit;
    private final String prefix;
    private final PrintStream output;
    private final Locale locale;
    private final Clock clock;
    private final DateFormat dateFormat;

    private MetricReporter(MetricRegistry registry, //
        PrintStream output, //
        String prefix, //
        Locale locale, //
        Clock clock, //
        TimeZone timeZone, //
        TimeUnit rateUnit, //
        TimeUnit durationUnit, //
        MetricFilter filter, //
        Set<MetricAttribute> disabledMetricAttributes) {
        this.registry = registry;
        this.output = output;
        this.prefix = prefix;
        this.locale = locale;
        this.clock = clock;
        this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
        this.dateFormat.setTimeZone(timeZone);
        this.rateFactor = rateUnit.toSeconds(1);
        this.rateUnit = calculateRateUnit(rateUnit);
        this.durationFactor = durationUnit.toNanos(1);
        this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        this.filter = filter;
        this.disabledMetricAttributes = disabledMetricAttributes != null ? disabledMetricAttributes : Collections
            .emptySet();
    }

    public void report(final SortedMap<String, Gauge> gauges, final SortedMap<String, Counter> counters,
        final SortedMap<String, Histogram> histograms, final SortedMap<String, Meter> meters,
        final SortedMap<String, Timer> timers) {
        final String dateTime = this.dateFormat.format(new Date(this.clock.getTime()));
        printWithBanner(dateTime, '=');
        this.output.println();

        if (!gauges.isEmpty()) {
            printWithBanner("-- Gauges", '-');
            for (final Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                this.output.println(entry.getKey());
                printGauge(entry.getValue());
            }
            this.output.println();
        }

        if (!counters.isEmpty()) {
            printWithBanner("-- Counters", '-');
            for (final Map.Entry<String, Counter> entry : counters.entrySet()) {
                this.output.println(entry.getKey());
                printCounter(entry);
            }
            this.output.println();
        }

        if (!histograms.isEmpty()) {
            printWithBanner("-- Histograms", '-');
            for (final Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                this.output.println(entry.getKey());
                printHistogram(entry.getValue());
            }
            this.output.println();
        }

        if (!meters.isEmpty()) {
            printWithBanner("-- Meters", '-');
            for (final Map.Entry<String, Meter> entry : meters.entrySet()) {
                this.output.println(entry.getKey());
                printMeter(entry.getValue());
            }
            this.output.println();
        }

        if (!timers.isEmpty()) {
            printWithBanner("-- Timers", '-');
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                this.output.println(entry.getKey());
                printTimer(entry.getValue());
            }
            this.output.println();
        }

        this.output.println();
        this.output.flush();
    }

    private void printMeter(final Meter meter) {
        printIfEnabled(MetricAttribute.COUNT, String.format(this.locale, "             count = %d", meter.getCount()));
        printIfEnabled(MetricAttribute.MEAN_RATE, String.format(this.locale, "         mean rate = %2.2f events/%s",
            convertRate(meter.getMeanRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M1_RATE, String.format(this.locale, "     1-minute rate = %2.2f events/%s",
            convertRate(meter.getOneMinuteRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M5_RATE, String.format(this.locale, "     5-minute rate = %2.2f events/%s",
            convertRate(meter.getFiveMinuteRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M15_RATE, String.format(this.locale, "    15-minute rate = %2.2f events/%s",
            convertRate(meter.getFifteenMinuteRate()), this.rateUnit));
    }

    private void printCounter(final Map.Entry<String, Counter> entry) {
        this.output.printf(this.locale, "             count = %d%n", entry.getValue().getCount());
    }

    private void printGauge(final Gauge<?> gauge) {
        this.output.printf(this.locale, "             value = %s%n", gauge.getValue());
    }

    private void printHistogram(final Histogram histogram) {
        printIfEnabled(MetricAttribute.COUNT,
            String.format(this.locale, "             count = %d", histogram.getCount()));
        final Snapshot snapshot = histogram.getSnapshot();
        printIfEnabled(MetricAttribute.MIN, String.format(this.locale, "               min = %d", snapshot.getMin()));
        printIfEnabled(MetricAttribute.MAX, String.format(this.locale, "               max = %d", snapshot.getMax()));
        printIfEnabled(MetricAttribute.MEAN,
            String.format(this.locale, "              mean = %2.2f", snapshot.getMean()));
        printIfEnabled(MetricAttribute.STDDEV,
            String.format(this.locale, "            stddev = %2.2f", snapshot.getStdDev()));
        printIfEnabled(MetricAttribute.P50,
            String.format(this.locale, "            median = %2.2f", snapshot.getMedian()));
        printIfEnabled(MetricAttribute.P75,
            String.format(this.locale, "              75%% <= %2.2f", snapshot.get75thPercentile()));
        printIfEnabled(MetricAttribute.P95,
            String.format(this.locale, "              95%% <= %2.2f", snapshot.get95thPercentile()));
        printIfEnabled(MetricAttribute.P98,
            String.format(this.locale, "              98%% <= %2.2f", snapshot.get98thPercentile()));
        printIfEnabled(MetricAttribute.P99,
            String.format(this.locale, "              99%% <= %2.2f", snapshot.get99thPercentile()));
        printIfEnabled(MetricAttribute.P999,
            String.format(this.locale, "            99.9%% <= %2.2f", snapshot.get999thPercentile()));
    }

    private void printTimer(final Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();
        printIfEnabled(MetricAttribute.COUNT, String.format(this.locale, "             count = %d", timer.getCount()));
        printIfEnabled(MetricAttribute.MEAN_RATE, String.format(this.locale, "         mean rate = %2.2f calls/%s",
            convertRate(timer.getMeanRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M1_RATE, String.format(this.locale, "     1-minute rate = %2.2f calls/%s",
            convertRate(timer.getOneMinuteRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M5_RATE, String.format(this.locale, "     5-minute rate = %2.2f calls/%s",
            convertRate(timer.getFiveMinuteRate()), this.rateUnit));
        printIfEnabled(MetricAttribute.M15_RATE, String.format(this.locale, "    15-minute rate = %2.2f calls/%s",
            convertRate(timer.getFifteenMinuteRate()), this.rateUnit));

        printIfEnabled(MetricAttribute.MIN, String.format(this.locale, "               min = %2.2f %s",
            convertDuration(snapshot.getMin()), this.durationUnit));
        printIfEnabled(MetricAttribute.MAX, String.format(this.locale, "               max = %2.2f %s",
            convertDuration(snapshot.getMax()), this.durationUnit));
        printIfEnabled(MetricAttribute.MEAN, String.format(this.locale, "              mean = %2.2f %s",
            convertDuration(snapshot.getMean()), this.durationUnit));
        printIfEnabled(MetricAttribute.STDDEV, String.format(this.locale, "            stddev = %2.2f %s",
            convertDuration(snapshot.getStdDev()), this.durationUnit));
        printIfEnabled(MetricAttribute.P50, String.format(this.locale, "            median = %2.2f %s",
            convertDuration(snapshot.getMedian()), this.durationUnit));
        printIfEnabled(MetricAttribute.P75, String.format(this.locale, "              75%% <= %2.2f %s",
            convertDuration(snapshot.get75thPercentile()), this.durationUnit));
        printIfEnabled(MetricAttribute.P95, String.format(this.locale, "              95%% <= %2.2f %s",
            convertDuration(snapshot.get95thPercentile()), this.durationUnit));
        printIfEnabled(MetricAttribute.P98, String.format(this.locale, "              98%% <= %2.2f %s",
            convertDuration(snapshot.get98thPercentile()), this.durationUnit));
        printIfEnabled(MetricAttribute.P99, String.format(this.locale, "              99%% <= %2.2f %s",
            convertDuration(snapshot.get99thPercentile()), this.durationUnit));
        printIfEnabled(MetricAttribute.P999, String.format(this.locale, "            99.9%% <= %2.2f %s",
            convertDuration(snapshot.get999thPercentile()), this.durationUnit));
    }

    private void printWithBanner(final String s, final char c) {
        if (!this.prefix.isEmpty()) {
            this.output.print(this.prefix);
            this.output.print(' ');
        }
        this.output.print(s);
        this.output.print(' ');
        for (int i = 0; i < (CONSOLE_WIDTH - s.length() - 1); i++) {
            this.output.print(c);
        }
        this.output.println();
    }

    /**
     * Print only if the attribute is enabled
     *
     * @param type Metric attribute
     * @param status Status to be logged
     */
    private void printIfEnabled(final MetricAttribute type, final String status) {
        if (this.disabledMetricAttributes.contains(type)) {
            return;
        }

        this.output.println(status);
    }

    private String calculateRateUnit(final TimeUnit unit) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }

    private double convertRate(final double rate) {
        return rate * this.rateFactor;
    }

    private double convertDuration(final double duration) {
        return duration / this.durationFactor;
    }
}
