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

package org.apache.ignite.internal.processors.security.sandbox;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * This test shows that an user-defined code with a privileged block cannot execute a secure-sensitive operation.
 */
public class DoPrivilegedOnRemoteNodeTest extends AbstractSandboxTest {
    /** */
    private static final String CALLABLE_DO_PRIVELEGED_SRC =
        "import java.security.AccessController;\n" +
            "import java.security.PrivilegedAction;\n" +
            "import org.apache.ignite.lang.IgniteCallable;\n" +
            "\n" +
            "public class TestDoPrivilegedIgniteCallable implements IgniteCallable<String> {\n" +
            "    public String call() throws Exception {\n" +
            "        return AccessController.doPrivileged(\n" +
            "            (PrivilegedAction<String>)() -> System.getProperty(\"user.home\")\n" +
            "        );\n" +
            "    }\n" +
            "}";

    /** */
    private static final String CALLABLE_SECURITY_UTILS_SRC =
        "import org.apache.ignite.internal.processors.security.SecurityUtils;\n" +
            "import org.apache.ignite.lang.IgniteCallable;\n" +
            "\n" +
            "public class TestSecurityUtilsCallable implements IgniteCallable<String> {\n" +
            "    @Override public String call() throws Exception {\n" +
            "        return SecurityUtils.doPrivileged(() ->{\n" +
            "            return System.getProperty(\"user.home\");\n" +
            "        });\n" +
            "    }\n" +
            "}";

    /** */
    private Path srcTmpDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDeploymentSpi(new LocalDeploymentSpi());
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

    /** */
    @Test
    public void test() throws Exception {
        Ignite srv = startGrid("srv", ALLOW_ALL,  false);

        Ignite clnt = startGrid("clnt", ALLOW_ALL, true);

        srv.cluster().active(true);

        IgniteCompute compute = clnt.compute(clnt.cluster().forRemotes());

        checkCallable(compute, callable("TestDoPrivilegedIgniteCallable", CALLABLE_DO_PRIVELEGED_SRC));
        checkCallable(compute, callable("TestSecurityUtilsCallable", CALLABLE_SECURITY_UTILS_SRC));
    }

    /** */
    private void checkCallable(IgniteCompute compute, IgniteCallable<String> c) throws Exception {
        assertEquals(c.call(), System.getProperty("user.home"));

        assertThrowsWithCause(() -> compute.broadcast(c), AccessControlException.class);
    }

    /** */
    IgniteCallable<String> callable(String clsName, String src) {
        try {
            Files.createDirectories(srcTmpDir);

            File srcFile = new File(srcTmpDir.toFile(), clsName + ".java");

            Path srcFilePath = Files.write(srcFile.toPath(), src.getBytes(StandardCharsets.UTF_8));

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

            compiler.run(null, null, null, srcFilePath.toString());

            assertTrue("Failed to remove source file.", srcFile.delete());

            URLClassLoader clsLdr = new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});

            Class<?> cls = clsLdr.loadClass(clsName);

            return (IgniteCallable<String>)cls.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
