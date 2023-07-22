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

package org.apache.ignite.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.metric.MetricCommand;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.regex.Pattern.quote;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.management.SystemViewCommand.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.IGNITE_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.SystemViewCommandTest.NODE_ID;

/** Tests output of {@link MetricCommand} command. */
public class MetricCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Command line argument for printing metric values. */
    private static final String CMD_METRIC = "--metric";

    /** */
    private static final String CONFIGURE_HISTOGRAM = "--configure-histogram";

    /** */
    private static final String CONFIGURE_HITRATE = "--configure-hitrate";

    /** Test node with 0 index. */
    private IgniteEx ignite0;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();

        autoConfirmation = false;

        ignite0 = ignite(0);
    }

    /** Tests command error output in case of mandatory metric name is omitted. */
    @Test
    public void testMetricNameMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC),
            "Argument name required.");
    }

    /** Tests command error output in case value of {@code --node-id} argument is omitted. */
    @Test
    public void testNodeIdMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, SYS_METRICS, NODE_ID),
            "Please specify a value for argument: " + NODE_ID);
    }

    /** Tests command error output in case value of {@code --node-id} argument is invalid.*/
    @Test
    public void testInvalidNodeIdFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, SYS_METRICS, NODE_ID, "invalid_node_id"),
            "Failed to parse " + NODE_ID +
                " command argument. String representation of \"java.util.UUID\" is exepected." +
                " For example: 123e4567-e89b-42d3-a456-556642440000"
        );
    }

    /** Tests command error output in case multiple metric names are specified. */
    @Test
    public void testMultipleMetricNamesFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, IGNITE_METRICS, SYS_METRICS),
            "Unexpected argument: sys");
    }

    /** Tests command error output in case {@code --node-id} argument value refers to nonexistent node. */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, "--node-id", incorrectNodeId, IGNITE_METRICS),
            "Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found");
    }

    /** Tests command output in case nonexistent metric name is specified. */
    @Test
    public void testNonExistentMetric() {
        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_METRIC, IGNITE_METRICS + SEPARATOR),
            "No metric with specified name was found [name=" + IGNITE_METRICS + SEPARATOR + ']');

        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_METRIC, "nonexistent.metric"),
            "No metric with specified name was found [name=nonexistent.metric]");
    }

    /** Tests command error output in case of invalid arguments for configure command. */
    @Test
    public void testInvalidConfigureMetricParameter() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HISTOGRAM),
            "Argument name required");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HITRATE),
            "Argument name required");

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HISTOGRAM, "some.metric"),
            "Argument newBounds required"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HITRATE, "some.metric"),
            "Argument newRateTimeInterval required"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HISTOGRAM, "some.metric", "not_a_number"),
            "Can't parse number 'not_a_number', expected type: java.lang.Long"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HITRATE, "some.metric", "not_a_number"),
            "Can't parse number 'not_a_number', expected type: java.lang.Long"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HISTOGRAM, "some.metric", "1,not_a_number"),
            "Can't parse number 'not_a_number', expected type: java.lang.Long"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, CONFIGURE_HISTOGRAM, "some.metric", "3,2,1"),
            "Bounds must be sorted"
        );

        assertContains(
            log,
            executeCommand(
                EXIT_CODE_INVALID_ARGUMENTS,
                CMD_METRIC,
                CONFIGURE_HISTOGRAM,
                "some.metric",
                "1,2,3",
                CONFIGURE_HITRATE
            ),
            "Unexpected argument: --configure-hitrate"
        );
    }

    /** Tests configuration of histgoram metric. */
    @Test
    public void testConfigureHistogram() {
        String mregName = "configure-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        long[] bounds = new long[] {50, 500};

        HistogramMetricImpl histogram = mreg.histogram("histogram", bounds, null);

        bounds = histogram.bounds();

        assertEquals(2, bounds.length);
        assertEquals(50, bounds[0]);
        assertEquals(500, bounds[1]);

        executeCommand(EXIT_CODE_OK, CMD_METRIC, CONFIGURE_HISTOGRAM, histogram.name(), "1,2,3");

        bounds = histogram.bounds();

        assertEquals(3, bounds.length);
        assertEquals(1, bounds[0]);
        assertEquals(2, bounds[1]);
        assertEquals(3, bounds[2]);
    }

    /** Tests configuration of hitrate metric. */
    @Test
    public void testConfigureHitrate() {
        String mregName = "configure-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        HitRateMetric hitrate = mreg.hitRateMetric("hitrate", null, 500, 5);

        assertEquals(500, hitrate.rateTimeInterval());

        executeCommand(EXIT_CODE_OK, CMD_METRIC, CONFIGURE_HITRATE, hitrate.name(), "100");

        assertEquals(100, hitrate.rateTimeInterval());
    }

    /** */
    @Test
    public void testHistogramMetrics() {
        String mregName = "histogram-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        long[] bounds = new long[] {50, 500};

        HistogramMetricImpl histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        histogram = mreg.histogram("histogram_with_underscore", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        assertEquals("1", metric(ignite0, metricName(mregName, "histogram_0_50")));
        assertEquals("2", metric(ignite0, metricName(mregName, "histogram_50_500")));
        assertEquals("3", metric(ignite0, metricName(mregName, "histogram_500_inf")));
        assertEquals("[1, 2, 3]", metric(ignite0, metricName(mregName, "histogram")));

        assertEquals("1", metric(ignite0, metricName(mregName, "histogram_with_underscore_0_50")));
        assertEquals("2", metric(ignite0, metricName(mregName, "histogram_with_underscore_50_500")));
        assertEquals("3", metric(ignite0, metricName(mregName, "histogram_with_underscore_500_inf")));
        assertEquals("[1, 2, 3]", metric(ignite0, metricName(mregName, "histogram_with_underscore")));
    }

    /** */
    @Test
    public void testNodeIdArgument() {
        String mregName = "boolean-metric-registry";

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.booleanMetric("boolean-metric", "");

        mreg = ignite(1).context().metric().registry(mregName);

        mreg.booleanMetric("boolean-metric", "").value(true);

        assertEquals("false", metric(ignite0, metricName(mregName, "boolean-metric")));
        assertEquals("true", metric(ignite(1), metricName(mregName, "boolean-metric")));
    }

    /** */
    @Test
    public void testRegistryMetrics() {
        String mregName = "test-metric-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.booleanMetric("boolean-metric", "");
        mreg.longMetric("long-metric", "").increment();
        mreg.intMetric("int-metric", "").increment();
        mreg.doubleMetric("double-metric", "");
        mreg.hitRateMetric("hitrate-metric", "", getTestTimeout(), 2);
        mreg.histogram("histogram", new long[] {50, 100}, null).value(10);
        mreg.hitRateMetric("hitrate-metric", "", getTestTimeout(), 2);
        mreg.objectMetric("object-metric", Object.class, "").value(new Object() {
            @Override public String toString() {
                return "test-object";
            }
        });

        Map<String, String> metrics = metrics(ignite0, mregName);

        assertEquals("0.0", metrics.get(metricName(mregName, "double-metric")));
        assertEquals("false", metrics.get(metricName(mregName, "boolean-metric")));
        assertEquals("1", metrics.get(metricName(mregName, "long-metric")));
        assertEquals("1", metrics.get(metricName(mregName, "int-metric")));
        assertEquals("test-object", metrics.get(metricName(mregName, "object-metric")));
        assertEquals("[1, 0, 0]", metrics.get(metricName(mregName, "histogram")));
        assertEquals("0", metric(ignite0, metricName(mregName, "hitrate-metric")));
    }

    /** */
    @Test
    public void testBooleanMetrics() {
        String mregName = "boolean-metric-registry";

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.booleanMetric("boolean-metric", "");

        assertEquals("false", metric(ignite0, metricName(mregName, "boolean-metric")));

        mreg.register("boolean-gauge", () -> true, "");

        assertEquals("true", metric(ignite0, metricName(mregName, "boolean-gauge")));
    }

    /** */
    @Test
    public void testLongMetrics() {
        String mregName = "long-metric-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.longMetric("long-metric", "").add(Long.MAX_VALUE);

        assertEquals(Long.toString(Long.MAX_VALUE), metric(ignite0, metricName(mregName, "long-metric")));

        mreg.register("long-gauge", () -> 0L, "");

        assertEquals("0", metric(ignite0, metricName(mregName, "long-gauge")));
    }

    /** */
    @Test
    public void testIntegerMetrics() {
        String mregName = "int-metric-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.intMetric("int-metric", "").add(Integer.MAX_VALUE);

        assertEquals(Integer.toString(Integer.MAX_VALUE), metric(ignite0, metricName(mregName, "int-metric")));

        mreg.register("int-gauge", () -> 0, "");

        assertEquals("0", metric(ignite0, metricName(mregName, "int-gauge")));
    }

    /** */
    @Test
    public void testDoubleMetrics() {
        String mregName = "int-double-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.doubleMetric("double-metric", "").add(111.222);

        assertEquals("111.222", metric(ignite0, metricName(mregName, "double-metric")));

        mreg.register("double-gauge", () -> 0D, "");

        assertEquals("0.0", metric(ignite0, metricName(mregName, "double-gauge")));
    }

    /** */
    @Test
    public void testObjectMetrics() {
        String mregName = "object-registry";

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        Object metricVal = new Object() {
            @Override public String toString() {
                return "test-object";
            }
        };

        mreg.objectMetric("object-metric", Object.class, "").value(metricVal);

        assertEquals("test-object", metric(ignite0, metricName(mregName, "object-metric")));

        mreg.register("object-gauge", () -> metricVal, Object.class, "");

        assertEquals("test-object", metric(ignite0, metricName(mregName, "object-gauge")));
    }

    /** */
    @Test
    public void testHitrateMetrics() {
        String mregName = "hitrate-registry";

        ignite0.context().metric().remove(mregName);

        MetricRegistry mreg = ignite0.context().metric().registry(mregName);

        mreg.hitRateMetric("hitrate-metric", "", getTestTimeout(), 2).add(Integer.MAX_VALUE);

        assertEquals(Integer.toString(Integer.MAX_VALUE), metric(ignite0, metricName(mregName, "hitrate-metric")));
    }

    /**
     * Gets metric values via command-line utility.
     *
     * @param node Node to obtain metric values from.
     * @param name Name of a particular metric or metric registry.
     * @return String representation of metric values.
     */
    private Map<String, String> metrics(IgniteEx node, String name) {
        String nodeId = node.context().discovery().localNode().id().toString();

        String out = executeCommand(EXIT_CODE_OK, CMD_METRIC, name, NODE_ID, nodeId);

        Map<String, String> res = parseMetricCommandOutput(out);

        assertEquals("value", res.remove("metric"));
        
        return res;
    }

    /**
     * Gets single metric value via command-line utility.
     *
     * @param node Node to obtain metric from.
     * @param name Name of the metric.
     * @return String representation of metric value.
     */
    private String metric(IgniteEx node, String name) {
        Map<String, String> metrics = metrics(node, name);
        
        assertEquals(1, metrics.size());
        
        return metrics.get(name);
    }

    /**
     * Obtains metric values from command output.
     *
     * @param out Command output to parse.
     * @return Metric values.
     */
    private Map<String, String> parseMetricCommandOutput(String out) {
        if (commandHandler.equals(CLI_CMD_HND)) {
            String outStart = "--------------------------------------------------------------------------------";

            String outEnd = "Command [METRIC] finished with code: " + EXIT_CODE_OK;

            out = out.substring(
                out.indexOf(outStart) + outStart.length() + 1,
                out.indexOf(outEnd) - 1
            );
        }

        String[] rows = out.split(U.nl());

        Map<String, String> res = new HashMap<>();

        for (String row : rows) {
            Iterator<String> iter = Arrays.stream(row.split(quote(COLUMN_SEPARATOR)))
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .iterator();

            res.put(iter.next(), iter.next());
        }
        
        return res;
    }
}
