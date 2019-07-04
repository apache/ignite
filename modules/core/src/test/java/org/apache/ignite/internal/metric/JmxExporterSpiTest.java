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

package org.apache.ignite.internal.metric;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.DynamicMBean;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.jmx.JmxExporterSpi;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class JmxExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        JmxExporterSpi jmxSpi = new JmxExporterSpi();

        jmxSpi.setExportFilter(m -> !m.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(jmxSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDataRegionJmxMetrics() throws Exception {
        DynamicMBean dataRegionMBean = metricSet("io", "dataregion.default");

        Set<String> res = stream(dataRegionMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.containsAll(EXPECTED_ATTRIBUTES));

        for (String metricName : res)
            assertNotNull(metricName, dataRegionMBean.getAttribute(metricName));
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        assertThrowsWithCause(new RunnableX() {
            @Override public void runx() throws Exception {
                metricSet("filtered", "metric");
            }
        }, IgniteException.class);

        DynamicMBean bean1 = metricSet("other", "prefix");

        assertEquals(42L, bean1.getAttribute("test"));
        assertEquals(43L, bean1.getAttribute("test2"));

        DynamicMBean bean2 = metricSet("other", "prefix2");

        assertEquals(44L, bean2.getAttribute("test3"));
    }

    /** */
    public DynamicMBean metricSet(String grp, String name) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(ignite.name(), grp, name);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            throw new IgniteException("MBean not registered.");

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }
}
