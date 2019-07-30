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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.PermissionsBuilder;
import org.apache.ignite.internal.processors.security.impl.TestStoreFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static sun.security.util.SecurityConstants.MODIFY_THREADGROUP_PERMISSION;
import static sun.security.util.SecurityConstants.MODIFY_THREAD_PERMISSION;

/**
 * This test shows that an user-defined code with a privileged block cannot execute a secure-sensitive operation.
 */
public class DoPrivelegedOnRemoteNodeTest extends AbstractSandboxTest {
    /** */
    private static final String RUNNABLE_SRC = "import org.apache.ignite.lang.IgniteRunnable;\n" +
        "\n" +
        "import static org.apache.ignite.internal.processors.security.sandbox.AbstractSandboxTest.START_THREAD_RUNNABLE;\n" +
        "\n" +
        "public class TestIgniteRunnable implements IgniteRunnable {\n" +
        "    @Override public void run() {\n" +
        "        START_THREAD_RUNNABLE.run();\n" +
        "    }\n" +
        "}";

    /** */
    private static final String RUNNABLE_DO_PRIVELEGED_SRC = "import java.security.AccessController;\n" +
        "import java.security.PrivilegedActionException;\n" +
        "import java.security.PrivilegedExceptionAction;\n" +
        "import org.apache.ignite.lang.IgniteRunnable;\n" +
        "\n" +
        "import static org.apache.ignite.internal.processors.security.sandbox.AbstractSandboxTest.START_THREAD_RUNNABLE;\n" +
        "\n" +
        "public class TestDoPrivilegedIgniteRunnable implements IgniteRunnable {\n" +
        "    @Override public void run() {\n" +
        "        try {\n" +
        "            AccessController.doPrivileged(\n" +
        "                (PrivilegedExceptionAction<Object>)() -> {\n" +
        "                    START_THREAD_RUNNABLE.run();\n" +
        "                    return null;\n" +
        "                }\n" +
        "            );\n" +
        "        }\n" +
        "        catch (PrivilegedActionException e) {\n" +
        "            throw new RuntimeException(e.getException());\n" +
        "        }\n" +
        "    }\n" +
        "}";

    /** */
    private Path srcTmpDir;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(TEST_CACHE)
                    .setCacheStoreFactory(new TestStoreFactory("1", "val"))
            );
    }

    /** */
    @Test
    public void test() throws Exception {
        prepareCluster();

        Ignite clntAllowed = grid(CLNT_ALLOWED);

        IgniteCompute compute = clntAllowed.compute(clntAllowed.cluster().forRemotes());

        runOperation(() -> compute.broadcast(runnable("TestIgniteRunnable", RUNNABLE_SRC)));

        assertThrowsWithCause(
            () -> compute.broadcast(runnable("TestDoPrivilegedIgniteRunnable", RUNNABLE_DO_PRIVELEGED_SRC)),
            AccessControlException.class);
    }

    /** */
    @Override protected void prepareCluster() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        startGrid(CLNT_ALLOWED, ALLOW_ALL,
            PermissionsBuilder.create()
                .add(MODIFY_THREAD_PERMISSION)
                .add(MODIFY_THREADGROUP_PERMISSION).get(), true);

        srv.cluster().active(true);
    }

    /** */
    IgniteRunnable runnable(String clsName, String src) {
        try {
            URLClassLoader clsLdr = prepareClassLoader(clsName + ".java", src);

            Class<?> cls = clsLdr.loadClass(clsName);

            return (IgniteRunnable)cls.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private URLClassLoader prepareClassLoader(String clsName, String src) throws Exception {
        Files.createDirectories(srcTmpDir);

        File srcFile = new File(srcTmpDir.toFile(), clsName);

        Path srcFilePath = Files.write(srcFile.toPath(), src.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        compiler.run(null, null, null, srcFilePath.toString());

        assertTrue("Failed to remove source file.", srcFile.delete());

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()}, getClass().getClassLoader());
    }

}
