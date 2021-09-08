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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.Permissions;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.security.SecurityUtils.IGNITE_INTERNAL_PACKAGE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * The test shows: a subject that does not have appropriate {@code accessClassInPackage.{internal_package_name}} runtime
 * permissions cannot create an object (T2 class) from an internal package and does not have access to static methods of
 * internal classes.
 */
public class AccessToClassesInsideInternalPackageTest extends AbstractSandboxTest {
    /** */
    private static final String CREATE_INSTANCE_CLS_NAME = "TestIgniteCallable";

    /** Callable that creates an instance of T2 from internal Ignite package. */
    private static final String CREATE_INSTANCE_SRC = "import org.apache.ignite.lang.IgniteCallable;\n" +
        "import org.apache.ignite.internal.util.typedef.T2;\n" +
        "\n" +
        "public class TestIgniteCallable implements IgniteCallable {\n" +
        "       public Object call() throws Exception {\n" +
        "            return new T2<>(\"a\", \"b\");\n" +
        "        }" +
        "}";

    /** */
    private static final String CALL_INTERNAL_CLASS_METHOD_CLS_NAME = "TestInternalUtilsCallable";

    /** Callable that calls a static class method from internal Ignite package. */
    private static final String CALL_INTERNAL_CLASS_METHOD_SRC =
        "import org.apache.ignite.internal.IgnitionEx;\n" +
            "import org.apache.ignite.lang.IgniteCallable;\n" +
            "\n" +
            "public class TestInternalUtilsCallable implements IgniteCallable {\n" +
            "    @Override public Object call() throws Exception {\n" +
            "        return IgnitionEx.isDaemon();\n" +
            "    }\n" +
            "}";

    /** Node that has access to internal Ignite package. */
    private static final String ALLOWED = "allowed";

    /** Node that does not have access to internal Ignite package. */
    private static final String FORBIDDEN = "forbidden";

    /** */
    private Path srcTmpDir;

    /** */
    @Test
    public void testCreateInstance() {
        IgniteCallable<Object> c = callable(srcTmpDir, CREATE_INSTANCE_CLS_NAME, CREATE_INSTANCE_SRC);

        assertNotNull(compute(grid(ALLOWED)).call(c));

        assertThrowsWithCause(() -> compute(grid(FORBIDDEN)).call(c), AccessControlException.class);
    }

    /** */
    @Test
    public void testCallStaticMethod() {
        IgniteCallable<Object> c = callable(srcTmpDir, CALL_INTERNAL_CLASS_METHOD_CLS_NAME,
            CALL_INTERNAL_CLASS_METHOD_SRC);

        assertNotNull(compute(grid(ALLOWED)).call(c));

        assertThrowsWithCause(() -> compute(grid(FORBIDDEN)).call(c), AccessControlException.class);
    }

    /** {@inheritDoc} */
    @Override protected void prepareCluster() throws Exception {
        Ignite srv = startGrid("srv", ALLOW_ALL, false);

        Permissions perms = new Permissions();

        perms.add(new RuntimePermission("accessClassInPackage." + IGNITE_INTERNAL_PACKAGE));
        perms.add(new RuntimePermission("accessClassInPackage." + IGNITE_INTERNAL_PACKAGE + ".*"));

        startGrid(ALLOWED, ALLOW_ALL, perms, false);

        startGrid(FORBIDDEN, ALLOW_ALL, false);

        srv.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDeploymentSpi(new LocalDeploymentSpi());
    }

    /** */
    @Before
    public void setUp() throws Exception {
        srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());
    }

    /** */
    @After
    public void tearDown() {
        U.delete(srcTmpDir);
    }

    /**
     * @return IgniteCompute with the filter that allows SRV node only.
     */
    private IgniteCompute compute(Ignite node) {
        return node.compute(node.cluster().forNodeId(grid(SRV).localNode().id()));
    }
}
