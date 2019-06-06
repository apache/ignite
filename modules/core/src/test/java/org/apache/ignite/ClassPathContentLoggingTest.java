/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP;

/**
 *
 */
@WithSystemProperty(key = IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP, value = "true")
public class ClassPathContentLoggingTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    private String javaClassPath;

    /** */
    private static String javaHome = System.getenv("JAVA_HOME");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        javaClassPath = System.getProperty("java.class.path", ".");

        assertNotNull(javaHome);

        StringBuilder jarPath = new StringBuilder(javaHome)
            .append(javaHome.endsWith(File.separator) ? "" : File.separator)
            .append("lib")
            .append(File.separator)
            .append("*");

        System.setProperty("java.class.path", javaClassPath + File.pathSeparator + jarPath.toString());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty("java.class.path", javaClassPath);

        super.afterTestsStopped();
    }

    /**
     * Checks the presence of class path content message in log when enabled.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testClassPathContentLogging() throws Exception {
        LogListener lsnr = LogListener
            .matches("List of files containing in classpath")
            .build();

        LogListener clsPathValuelsnr = LogListener
            .matches("Classpath value")
            .build();

        LogListener errLsnr = LogListener
            .matches("Could not log class path entry")
            .build();

        LogListener.Builder jarLsnrBuilder = LogListener.builder();

        String jarPath = new StringBuilder(javaHome)
            .append(javaHome.endsWith(File.separator) ? "" : File.separator)
            .append("lib")
            .append(File.separator)
            .toString();

        Iterable<Path> jars = Files.newDirectoryStream(Paths.get(jarPath), "*.jar");

        for (Path jar : jars)
            jarLsnrBuilder.andMatches(jar.getFileName().toString());

        LogListener jarLsnr = jarLsnrBuilder.build();

        listeningLog.registerListener(lsnr);
        listeningLog.registerListener(clsPathValuelsnr);
        listeningLog.registerListener(errLsnr);
        listeningLog.registerListener(jarLsnr);

        startGrid(0);

        assertTrue(lsnr.check());
        assertTrue(clsPathValuelsnr.check());
        assertTrue(jarLsnr.check());

        assertFalse(errLsnr.check());
    }
}
