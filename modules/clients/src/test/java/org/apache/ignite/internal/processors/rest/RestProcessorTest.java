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

package org.apache.ignite.internal.processors.rest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Rest processor test.
 * <p>
 * URLs to test:
 * <ul>
 * <li>http://localhost:8080/ignite?cmd=get&key=simpleBean</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=complexBean</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=list</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=map</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=int</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=string</li>
 * <li>http://localhost:8080/ignite?cmd=get&key=date</li>
 * <li>http://localhost:8080/ignite?cmd=top</li>
 * <li>http://localhost:8080/ignite?cmd=exe&name=org.apache.ignite.internal.processors.rest.TestTask2</li>
 * <li>http://localhost:8080/ignite?cmd=exe&name=org.apache.ignite.internal.processors.rest.TestTask2&async=true</li>
 * <li>http://localhost:8080/ignite?cmd=res&id=XXXX</li>
 * </ul>
 */
public class RestProcessorTest extends GridCommonAbstractTest {
    /** Counter */
    private static int cntr;

    /** */
    public RestProcessorTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRest() throws Exception {
        IgniteConfiguration cfg = getConfiguration((String)null);

        cfg = cacheTestConfiguration(cfg);

        G.start(cfg);

        populateCache();

        deployTasks();

        // Wait until Ok is pressed.
        JOptionPane.showMessageDialog(
            null,
            new JComponent[] {
                new JLabel("Ignite started."),
                new JLabel(
                    "<html>" +
                        "You can use JMX console at <u>http://localhost:1234</u>" +
                        "</html>"),
                new JLabel("Press OK to stop Ignite.")
            },
            "Ignite Startup JUnit",
            JOptionPane.INFORMATION_MESSAGE
        );

        G.stop(true);
    }

    /**
     * @param cfg Initial configuration.
     * @return Final configuration.
     */
    @SuppressWarnings({"unchecked"})
    private IgniteConfiguration cacheTestConfiguration(IgniteConfiguration cfg) {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setShared(true);

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        // Ensure - no authentication.
        clientCfg.setSecretKey(null);

        cfg.setConnectorConfiguration(clientCfg);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return Integer.
     */
    private int intValue() {
        return ++cntr;
    }

    /**
     *
     */
    private void populateCache() {
        IgniteCache<String, Object> cache = G.ignite().cache(null);

        cache.put("int", intValue());
        cache.put("string", "cacheString");
        cache.put("date", new Date());
        cache.put("list", createCollection());
        cache.put("map", createMap());
        cache.put("simpleBean", new SimpleBean());

        ComplexBean bean = new ComplexBean(new SimpleBean(intValue(), "complexSimpleString"));

        bean.setComplexBean(new ComplexBean(new SimpleBean(intValue(), "complexComplexString")));

        cache.put("complexBean", bean);
    }

    /**
     *
     */
    private void deployTasks() {
        G.ignite().compute().localDeployTask(TestTask1.class, TestTask1.class.getClassLoader());
        G.ignite().compute().localDeployTask(TestTask2.class, TestTask2.class.getClassLoader());
    }

    /**
     * @return Map.
     */
    private Map<?, ?> createMap() {
        Map<Object, Object> map = new HashMap<>();

        map.put("intValue", intValue());
        map.put("stringValue", "mapString");
        map.put("simpleBean", new SimpleBean());
        map.put("complexBean", new ComplexBean(new SimpleBean(intValue(), "mapSimpleComplexString")));

        Map<Object, Object> nested = new HashMap<>();

        nested.put("intValue", intValue());
        nested.put("stringValue", "nestedMapString");
        nested.put("simpleBean", new SimpleBean());
        nested.put("complexBean", new ComplexBean(new SimpleBean(intValue(), "mapSimpleComplexNestedString")));

        map.put("nestedMap", nested);

        return map;
    }

    /**
     * @return List.
     */
    private Collection<?> createCollection() {
        Collection<Object> list = new ArrayList<>();

        list.add(intValue());
        list.add("listString");
        list.add(new Date());

        Collection<Object> nested = new ArrayList<>();

        nested.add(intValue());
        nested.add("nestedListString");
        nested.add(new Date());

        list.add(nested);

        return list;
    }

    /**
     * Simple bean.
     */
    @SuppressWarnings( {"ReturnOfDateField", "AssignmentToDateFieldFromParameter", "PublicInnerClass"})
    public static class SimpleBean implements Serializable {
        /** */
        private int intField = 12345;

        /** */
        private String strField = "testString";

        /** */
        private Date date = new Date();

        /**
         * Empty constructor.
         */
        private SimpleBean() {
            // No-op.
        }

        /**
         * @param intField Int value.
         * @param strField String value.
         */
        private SimpleBean(int intField, String strField) {
            this.intField = intField;
            this.strField = strField;
        }

        /**
         * @param intField Int value.
         * @param strField String value.
         * @param date Date value.
         */
        private SimpleBean(int intField, String strField, Date date) {
            this.intField = intField;
            this.strField = strField;
            this.date = date;
        }

        /**
         * @return Int value.
         */
        public int getIntField() {
            return intField;
        }

        /**
         * @param intField Int value.
         */
        public void setIntField(int intField) {
            this.intField = intField;
        }

        /**
         * @return String value.
         */
        public String getStringField() {
            return strField;
        }

        /**
         * @param strField String value.
         */
        public void setStringField(String strField) {
            this.strField = strField;
        }

        /**
         * @return Date value.
         */
        public Date getDate() {
            return date;
        }

        /**
         * @param date Date value.
         */
        public void setDate(Date date) {
            this.date = date;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SimpleBean.class, this);
        }
    }

    /**
     * Simple bean.
     */
    @SuppressWarnings( {"ReturnOfDateField", "PublicInnerClass"})
    public static class ComplexBean extends SimpleBean {
        /** */
        private SimpleBean simpleBean = new SimpleBean(67890, "nestedTestString", new Date());

        /** */
        private ComplexBean complexBean;

        /**
         * @param simpleBean Simple bean.
         */
        private ComplexBean(SimpleBean simpleBean) {
            this.simpleBean = simpleBean;
        }

        /**
         * @return Simple bean.
         */
        public SimpleBean getSimpleBean() {
            return simpleBean;
        }

        /**
         * @param simpleBean Simple bean.
         */
        public void setSimpleBean(SimpleBean simpleBean) {
            this.simpleBean = simpleBean;
        }

        /**
         * @return Complex bean.
         */
        public ComplexBean getComplexBean() {
            return complexBean;
        }

        /**
         * @param complexBean Complex bean.
         */
        public void setComplexBean(ComplexBean complexBean) {
            this.complexBean = complexBean;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ComplexBean.class, this);
        }
    }
}