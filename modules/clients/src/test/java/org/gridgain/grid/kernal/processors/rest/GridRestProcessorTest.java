/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import javax.swing.*;
import java.io.*;
import java.util.*;

/**
 * Rest processor test.
 * <p>
 * URLs to test:
 * <ul>
 * <li>http://localhost:8080/gridgain?cmd=get&key=simpleBean</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=complexBean</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=list</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=map</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=int</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=string</li>
 * <li>http://localhost:8080/gridgain?cmd=get&key=date</li>
 * <li>http://localhost:8080/gridgain?cmd=top</li>
 * <li>http://localhost:8080/gridgain?cmd=exe&name=org.gridgain.grid.kernal.processors.rest.TestTask2</li>
 * <li>http://localhost:8080/gridgain?cmd=exe&name=org.gridgain.grid.kernal.processors.rest.TestTask2&async=true</li>
 * <li>http://localhost:8080/gridgain?cmd=res&id=XXXX</li>
 * </ul>
 */
public class GridRestProcessorTest extends GridCommonAbstractTest {
    /** Counter */
    private static int cntr;

    /** */
    public GridRestProcessorTest() {
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
                new JLabel("GridGain started."),
                new JLabel(
                    "<html>" +
                        "You can use JMX console at <u>http://localhost:1234</u>" +
                        "</html>"),
                new JLabel("Press OK to stop GridGain.")
            },
            "GridGain Startup JUnit",
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

        assert cfg.getClientConnectionConfiguration() == null;

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        // Ensure - no authentication.
        clientCfg.setRestSecretKey(null);

        cfg.setClientConnectionConfiguration(clientCfg);

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
     * @throws GridException If failed.
     */
    private void populateCache() throws GridException {
        GridCache<String, Object> cache = G.grid().cache(null);

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
     * @throws GridException If failed.
     */
    private void deployTasks() throws GridException {
        G.grid().compute().localDeployTask(TestTask1.class, TestTask1.class.getClassLoader());
        G.grid().compute().localDeployTask(TestTask2.class, TestTask2.class.getClassLoader());
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
