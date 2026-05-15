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

package org.apache.ignite.internal.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class IgniteBuildInfoPropertiesTest extends GridCommonAbstractTest {
    /** */
    private static final String PROPS_CLASS = "org.apache.ignite.internal.IgniteProperties";

    /** */
    private static final String VER_UTILS_CLASS = "org.apache.ignite.internal.IgniteVersionUtils";

    /** */
    @Test
    public void testLoadsBuildInfoProperties() {
        String ver = IgniteProperties.get("ignite.version");
        String build = IgniteProperties.get("ignite.build");
        String rev = IgniteProperties.get("ignite.revision");
        String relDate = IgniteProperties.get("ignite.rel.date");

        assertNotNull(ver);
        assertNotNull(build);
        assertNotNull(rev);
        assertNotNull(relDate);
    }

    /** */
    @Test
    public void testUserIgnitePropertiesDoesNotOverrideBuildMetadata() throws Exception {
        String wrongVerStr = "WRONG_VERSION";

        File tmpDir = Files.createTempDirectory("ignite-props-test-").toFile();

        try {
            File userProps = new File(tmpDir, "ignite.properties");

            try (FileWriter writer = new FileWriter(userProps)) {
                writer.write("ignite.version=" + wrongVerStr + U.nl());
                writer.write("some.custom.property=123" + U.nl());
            }

            URL tmpUrl = tmpDir.toURI().toURL();

            URL igniteCoreUrl = IgniteProperties.class.getProtectionDomain().getCodeSource().getLocation();

            URL[] urls = new URL[] {tmpUrl, igniteCoreUrl};

            ClassLoader ldr = IgniteProperties.class.getClassLoader();

            Set<String> urlLoadedClasses = Set.of(PROPS_CLASS, VER_UTILS_CLASS);

            try (TestURLClassLoader testLdr = new TestURLClassLoader(urls, ldr, urlLoadedClasses)) {
                URL userPropsUrl = testLdr.getResource("ignite.properties");

                assertNotNull(userPropsUrl);

                Properties props = new Properties();

                try (InputStream in = userPropsUrl.openStream()) {
                    props.load(in);
                }

                assertEquals(2, props.size());
                assertEquals(wrongVerStr, props.getProperty("ignite.version"));
                assertEquals("123", props.getProperty("some.custom.property"));

                Class<?> propsCls = Class.forName(PROPS_CLASS, true, testLdr);
                Class<?> utilsCls = Class.forName(VER_UTILS_CLASS, false, testLdr);

                assertSame(testLdr, propsCls.getClassLoader());
                assertSame(testLdr, utilsCls.getClassLoader());

                assertNotNull(testLdr.getResource("ignite.properties"));
                assertNotNull(testLdr.getResource("ignite-build-info.properties"));

                Method getMtd = propsCls.getMethod("get", String.class);

                String ver = (String)getMtd.invoke(null, "ignite.version");

                assertEquals("Build metadata must not be overridden by user ignite.properties",
                    IgniteVersionUtils.VER_STR, ver);
            }
        }
        finally {
            U.delete(tmpDir);
        }
    }

    /** */
    private static class TestURLClassLoader extends URLClassLoader {
        /** */
        private final Set<String> urlLoadedClasses;

        /** */
        public TestURLClassLoader(URL[] urls, ClassLoader parent, Set<String> urlLoadedClasses) {
            super(urls, parent);

            this.urlLoadedClasses = new HashSet<>(urlLoadedClasses);
        }

        /** {@inheritDoc} */
        @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (urlLoadedClasses.contains(name)) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> cls = findLoadedClass(name);

                    if (cls == null)
                        cls = findClass(name);

                    if (resolve)
                        resolveClass(cls);

                    return cls;
                }
            }

            return super.loadClass(name, resolve);
        }

        /** {@inheritDoc} */
        @Override public @Nullable URL getResource(String name) {
            URL url = findResource(name);

            return url != null ? url : super.getResource(name);
        }

        /** {@inheritDoc} */
        @Override public Enumeration<URL> getResources(String name) throws IOException {
            List<URL> res = new ArrayList<>();

            Enumeration<URL> childRes = findResources(name);

            while (childRes.hasMoreElements())
                res.add(childRes.nextElement());

            ClassLoader parent = getParent();

            if (parent != null) {
                Enumeration<URL> parentRes = parent.getResources(name);

                while (parentRes.hasMoreElements())
                    res.add(parentRes.nextElement());
            }

            return Collections.enumeration(res);
        }
    }
}
