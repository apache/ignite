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
import java.security.Security;
import java.util.UUID;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.impl.PermissionsBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.IGNITE_INTERNAL_PACKAGE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * The test shows: a subject that does not have accessClassInPackage.org.apache.ignite.internal.* runtime permission
 * cannot create object (T2 class) from an internal package.
 */
public class AccessClassInPackageTest extends AbstractSandboxTest {
    /** */
    private static final String CALLABLE_SRC = "import org.apache.ignite.lang.IgniteCallable;\n" +
        "import org.apache.ignite.internal.util.typedef.T2;\n" +
        "\n" +
        "public class TestIgniteCallable implements IgniteCallable {\n" +
        "       public Object call() throws Exception {\n" +
        "            return new T2<>(\"a\", \"b\");\n" +
        "        }" +
        "}";

    private static final String UTILS_CALLABLE = "import org.apache.ignite.internal.util.typedef.internal.U;\n" +
        "import org.apache.ignite.lang.IgniteCallable;\n" +
        "\n" +
        "public class PrivateUtilsCallable implements IgniteCallable {\n" +
        "    @Override public Object call() throws Exception {\n" +
        "        System.out.println(\"MY_DEBUG PrivateUtilsCallable\");\n" +
        "        return U.hexLong(101L);\n" +
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

        String packAccess = Security.getProperty("package.access");

        if (packAccess.contains(IGNITE_INTERNAL_PACKAGE)) {
            String[] strs = packAccess.split(",");

            StringBuilder sb = new StringBuilder();

            for (String s : strs) {
                if (!s.equals(IGNITE_INTERNAL_PACKAGE)) {
                    if (!sb.toString().isEmpty())
                        sb.append(',');
                    sb.append(s);
                }
            }

            Security.setProperty("package.access", sb.toString());
        }
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        //Node have permission.
        Ignite clntAllowed = startGrid(CLNT_ALLOWED, ALLOW_ALL,
            PermissionsBuilder.create()
                .add(new RuntimePermission("accessClassInPackage.org.apache.ignite.internal.*")).get(),
            true);

        //Node does not have permission.
        Ignite clntForbidden = startGrid(CLNT_FORBIDDEN, ALLOW_ALL, true);

        srv.cluster().active(true);

        UUID srvId = srv.cluster().localNode().id();

        //CLNT_ALLOWED node gets an instance of T2.
        T2<String, String> res = clntAllowed.compute(clntAllowed.cluster().forNodeId(srvId))
            .call(callable("TestIgniteCallable", CALLABLE_SRC));

        assertNotNull(res);

        Object o = clntAllowed.compute(clntAllowed.cluster().forNodeId(srvId))
            .call(callable("PrivateUtilsCallable", UTILS_CALLABLE));

        assertNotNull(o);

            //CLNT_FORBIDDEN node cannot create an instance of T2.
        assertThrowsWithCause(
            () -> clntForbidden.compute(clntForbidden.cluster().forNodeId(srvId))
                .call(callable("TestIgniteCallable", CALLABLE_SRC)),
            AccessControlException.class);

        assertThrowsWithCause(
            () -> clntForbidden.compute(clntForbidden.cluster().forNodeId(srvId))
                .call(callable("PrivateUtilsCallable", UTILS_CALLABLE)),
            AccessControlException.class);
    }

    /** */
    IgniteCallable<T2<String, String>> callable(String clsName, String src) {
        try {
            URLClassLoader clsLdr = prepareClassLoader(clsName + ".java", src);

            Class<?> cls = clsLdr.loadClass(clsName);

            return (IgniteCallable<T2<String, String>>)cls.newInstance();
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

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});
    }
}
