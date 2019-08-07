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

package org.apache.ignite.internal.processors.service;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests services hot redeployment via {@link DeploymentSpi}.
 *
 * <b>NOTE:</b> to run test via Maven Surefire Plugin, the property "forkCount' should be set great than '0' or profile
 * 'surefire-fork-count-1' enabled.
 */
public class ServiceHotRedeploymentViaDeploymentSpiTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private Path srcTmpDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentSpi(new LocalDeploymentSpi());

        return cfg;
    }

    /** */
    @BeforeClass
    public static void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** */
    @Before
    public void prepare() throws IOException {
        srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());
    }

    /** */
    @After
    public void cleanup() {
        U.delete(srcTmpDir);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void serviceHotRedeploymentTest() throws Exception {
        URLClassLoader clsLdr = prepareClassLoader(1);
        Class<?> cls = clsLdr.loadClass("MyRenewServiceImpl");

        MyRenewService srvc = (MyRenewService)cls.newInstance();

        assertEquals(1, srvc.version());

        try {
            Ignite ignite = startGrid(0);

            final DeploymentSpi depSpi = ignite.configuration().getDeploymentSpi();

            depSpi.register(clsLdr, srvc.getClass());

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);
            MyRenewService proxy = ignite.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false);
            assertEquals(1, proxy.version());

            ignite.services().cancel(SERVICE_NAME);
            depSpi.unregister(srvc.getClass().getName());

            clsLdr.close();

            clsLdr = prepareClassLoader(2);
            depSpi.register(clsLdr, srvc.getClass());

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);
            proxy = ignite.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false);
            assertEquals(2, proxy.version());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public interface MyRenewService extends Service {
        /**
         * @return Service's version.
         */
        public int version();
    }

    /**
     * @param ver Version of generated class.
     * @return Prepared classloader.
     * @throws Exception In case of an error.
     */
    private URLClassLoader prepareClassLoader(int ver) throws Exception {
        String source = "import org.apache.ignite.internal.processors.service.ServiceHotRedeploymentViaDeploymentSpiTest;\n" +
            "import org.apache.ignite.services.ServiceContext;\n" +
            "public class MyRenewServiceImpl implements ServiceHotRedeploymentViaDeploymentSpiTest.MyRenewService {\n" +
            "    @Override public int version() {\n" +
            "        return " + ver + ";\n" +
            "    }\n" +
            "    @Override public void cancel(ServiceContext ctx) {}\n" +
            "    @Override public void init(ServiceContext ctx) throws Exception {}\n" +
            "    @Override public void execute(ServiceContext ctx) throws Exception {}\n" +
            "}";

        Files.createDirectories(srcTmpDir); // To avoid possible NoSuchFileException on some OS.

        File srcFile = new File(srcTmpDir.toFile(), "MyRenewServiceImpl.java");

        Path srcFilePath = Files.write(srcFile.toPath(), source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        compiler.run(null, null, null, srcFilePath.toString());

        assertTrue("Failed to remove source file.", srcFile.delete());

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});
    }
}
