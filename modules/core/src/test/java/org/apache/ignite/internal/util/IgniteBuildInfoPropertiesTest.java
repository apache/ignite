package org.apache.ignite.internal.util;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgniteBuildInfoPropertiesTest extends GridCommonAbstractTest {
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
        File tmpDir = Files.createTempDirectory("ignite-props-test-").toFile();

        try {
            File userProps = new File(tmpDir, "ignite.properties");

            try (FileWriter writer = new FileWriter(userProps)) {
                writer.write("ignite.version=WRONG_VERSION" + U.nl());
                writer.write("some.custom.property=123" + U.nl());
            }

            URL[] urls = new URL[] {tmpDir.toURI().toURL()};

            ClassLoader ldr = IgniteProperties.class.getClassLoader();

            try (URLClassLoader testLdr = new URLClassLoader(urls, ldr)) {
                Class<?> cls = Class.forName("org.apache.ignite.internal.IgniteProperties", true, testLdr);

                Method getMtd = cls.getMethod("get", String.class);

                String ver = (String)getMtd.invoke(null, "ignite.version");

                assertEquals("Build metadata must not be overridden by user ignite.properties",
                    IgniteVersionUtils.VER_STR, ver);
            }
        }
        finally {
            U.delete(tmpDir);
        }
    }
}
