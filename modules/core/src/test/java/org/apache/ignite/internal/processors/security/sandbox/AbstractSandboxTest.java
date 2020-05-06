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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.util.PropertyPermission;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public abstract class AbstractSandboxTest extends AbstractSecurityTest {
    /** */
    protected static final String TEST_CACHE = "test_cache";

    /** */
    protected static boolean setupSM;

    /** Sever node name. */
    protected static final String SRV = "srv";

    /** Client node that can write to test property. */
    protected static final String CLNT_ALLOWED_WRITE_PROP = "clnt_allowed";

    /** Client node that cannot write to the test property. */
    protected static final String CLNT_FORBIDDEN_WRITE_PROP = "clnt_forbidden";

    /** Test property name. */
    private static final String PROP_NAME = "test.sandbox.property";

    /** Test property value. */
    private static final String PROP_VALUE = "propertyValue";

    /** */
    protected static void controlAction() {
        System.setProperty(PROP_NAME, PROP_VALUE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions res = new Permissions();

                    res.add(new AllPermission());

                    return res;
                }
            });

            System.setSecurityManager(new SecurityManager());

            setupSM = true;
        }

        prepareCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        if (setupSM) {
            System.setSecurityManager(null);
            Policy.setPolicy(null);
        }
    }

    /** */
    protected void prepareCluster() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        Permissions perms = new Permissions();

        perms.add(new PropertyPermission(PROP_NAME, "write"));

        startGrid(CLNT_ALLOWED_WRITE_PROP, ALLOW_ALL, perms, true);

        startGrid(CLNT_FORBIDDEN_WRITE_PROP, ALLOW_ALL, true);

        srv.cluster().active(true);
    }

    /**
     * @param r Runnable that runs {@link AbstractSandboxTest#controlAction()}.
     */
    protected void runOperation(Runnable r) {
        System.clearProperty(PROP_NAME);

        r.run();

        assertEquals(PROP_VALUE, System.getProperty(PROP_NAME));
    }

    /**
     * @param r RunnableX that that runs {@link AbstractSandboxTest#controlAction()}.
     */
    protected void runForbiddenOperation(GridTestUtils.RunnableX r, Class<? extends Throwable> cls) {
        System.clearProperty(PROP_NAME);

        assertThrowsWithCause(r, cls);

        assertNull(System.getProperty(PROP_NAME));
    }

    /**
     * Creates instance of IgniteCallable from passed src string.
     */
    protected <T> IgniteCallable<T> callable(Path srcTmpDir, String clsName, String src) {
        try {
            URLClassLoader clsLdr = prepareClassLoader(srcTmpDir, clsName + ".java", src);

            Class<?> cls = clsLdr.loadClass(clsName);

            return (IgniteCallable<T>)cls.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Prepares class loader.
     */
    private URLClassLoader prepareClassLoader(Path srcTmpDir, String clsName, String src) throws Exception {
        Files.createDirectories(srcTmpDir);

        File srcFile = new File(srcTmpDir.toFile(), clsName);

        Path srcFilePath = Files.write(srcFile.toPath(), src.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        compiler.run(null, null, null, srcFilePath.toString());

        assertTrue("Failed to remove source file.", srcFile.delete());

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});
    }
}
