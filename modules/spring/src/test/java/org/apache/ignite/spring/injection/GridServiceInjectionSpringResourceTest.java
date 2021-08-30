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

package org.apache.ignite.spring.injection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests for injected service.
 */
public class GridServiceInjectionSpringResourceTest extends GridCommonAbstractTest {
    /** Service name. */
    private static final String SERVICE_NAME = "testService";

    /** Bean name. */
    private static final String DUMMY_BEAN = "dummyResourceBean";

    /** */
    private static final int NODES = 8;

    /** */
    private static final int TEST_ITERATIONS = 5;

    /** */
    private static FileSystem FS = FileSystems.getDefault();

    /** */
    private static final String springCfgFileTemplate = "spring-resource.tmpl.xml";

    /** */
    private static String springCfgFileOutTmplName = "spring-resource.xml-";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        for (int i = 0; i < NODES; ++i)
            Files.deleteIfExists(FS.getPath(springCfgFileOutTmplName + i));

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployServiceWithSpring() throws Exception {
        generateConfigXmls(NODES);

        for (int i = 0; i < TEST_ITERATIONS; ++i) {
            log.info("Iteration: " + i);

            doOneTestIteration();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doOneTestIteration() throws Exception {
        startNodes();

        for (int i = 0; i < 10; ++i) {
            for (Ignite ignite : G.allGrids())
                ignite.compute().call(new TestJob());
        }

        stopAllGrids();
    }

    /**
     *
     * @throws Exception If failed.
     */
    private void startNodes() throws Exception {
        AbstractApplicationContext ctxSpring = new FileSystemXmlApplicationContext(springCfgFileOutTmplName + 0);

        // We have to deploy Services for service is available at the bean creation time for other nodes.
        Ignite ignite = (Ignite)ctxSpring.getBean("testIgnite");

        ignite.services().deployMultiple(SERVICE_NAME, new DummyServiceImpl(), NODES, 1);

        // Add other nodes.
        for (int i = 1; i < NODES; ++i)
            new FileSystemXmlApplicationContext(springCfgFileOutTmplName + i);

        assertEquals(NODES, G.allGrids().size());
        assertEquals(NODES, ignite.cluster().nodes().size());
    }

    /**
     * @param nodes Nodes.
     * @throws IOException on read/write config error
     */
    private void generateConfigXmls(int nodes) throws IOException {
        StringBuilder cfg = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(springCfgFileTemplate)))) {
            while (true) {
                String str = br.readLine();

                if (str == null)
                    break;

                cfg.append(str);
            }
        }

        for (int i = 0; i < nodes; ++i) {
            try (BufferedWriter bw = Files.newBufferedWriter(FS.getPath(springCfgFileOutTmplName + i), UTF_8)) {
                String str = cfg.toString();

                str = str.replaceAll("@GRID_IDX@", Integer.toString(i));

                bw.write(str);
            }
        }
    }

    /**
     *
     */
    private static class TestJob implements IgniteCallable {
        /** */
        @ServiceResource(serviceName = SERVICE_NAME, proxyInterface = DummyService.class)
        private DummyService svc;

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            assertNotNull(svc);

            svc.noop();

            return null;
        }
    }

    /**
     * Dummy Service.
     */
    public interface DummyService {
        /**
         *
         */
        void noop();
    }

    /**
     * No-op test service.
     */
    public static class DummyServiceImpl implements DummyService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SpringResource(resourceName = DUMMY_BEAN)
        private transient DummyResourceBean dummyRsrcBean;

        /** {@inheritDoc} */
        @Override public void noop() {
            System.out.println("DummyServiceImpl.noop()");
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            System.out.println("Cancelling service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            System.out.println("Initializing service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            System.out.println("Executing service: " + ctx.name());
        }
    }

    /**
     * Dummy resource bean.
     */
    public static class DummyResourceBean {
        /** */
        private transient Ignite ignite;

        /**
         * @return Ignite.
         */
        public Ignite getIgnite() {
            return ignite;
        }

        /**
         * @param ignite Ignite.
         */
        public void setIgnite(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * @throws Exception If failed.
         */
        @EventListener
        public void init(ContextRefreshedEvent evt) throws Exception {
            DummyService srv = ignite.services().serviceProxy(SERVICE_NAME, DummyService.class, false);

            assertNotNull(srv);
        }
    }
}
