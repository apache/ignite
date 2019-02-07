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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.Service;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ServiceHotRedeploymentTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void serviceDeploymentSpiTest() throws Exception {
        ClassLoader clsLdr = classLoader(1);
        Class<?> cls = clsLdr.loadClass("MyRenewServiceImpl");

        MyRenewService srvc = (MyRenewService)cls.newInstance();

        assertEquals(1, srvc.version());

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        LocalDeploymentSpi locDepSpi = new LocalDeploymentSpi();

        cfg.setDeploymentSpi(locDepSpi);

        try {
            Ignite ignite = startGrid(cfg);

            locDepSpi.register(clsLdr, srvc.getClass());

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);
            MyRenewService proxy = ignite.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false);
            assertEquals(1, proxy.version());

            ignite.services().cancel(SERVICE_NAME);
            locDepSpi.unregister(srvc.getClass().getName());

            clsLdr = classLoader(2);
            locDepSpi.register(clsLdr, srvc.getClass());

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
        public int version();
    }

    /**
     * @param ver Version of generated class.
     * @return Prepared classloader.
     * @throws Exception In case of an error.
     */
    private ClassLoader classLoader(int ver) throws Exception {
        String source = "import org.apache.ignite.internal.processors.service.ServiceHotRedeploymentTest;\n" +
            "import org.apache.ignite.services.ServiceContext;\n" +
            "public class MyRenewServiceImpl implements ServiceHotRedeploymentTest.MyRenewService {\n" +
            "    @Override public int version() {\n" +
            "        return " + ver + ";\n" +
            "    }\n" +
            "    @Override public void cancel(ServiceContext ctx) {}\n" +
            "    @Override public void init(ServiceContext ctx) throws Exception {}\n" +
            "    @Override public void execute(ServiceContext ctx) throws Exception {}\n" +
            "}";

        Path srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());
        File srcFile = new File(srcTmpDir.toFile(), "MyRenewServiceImpl.java");
        Path srcFilePath = Files.write(srcFile.toPath(), source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, srcFilePath.toString());

        return URLClassLoader.newInstance(new URL[] {srcTmpDir.toUri().toURL()});
    }
}
