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
package org.apache.ignite;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 *
 */
@WithSystemProperty(key = IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP, value = "true")
@WithSystemProperty(key = IGNITE_QUIET, value = "false")
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
        String javaClassPath = System.getProperty("java.class.path", ".");

        LogListener lsnr = LogListener
            .matches("List of files containing in classpath")
            .build();

        LogListener clsPathValuelsnr = LogListener
            .matches("Classpath value")
            .build();

        LogListener errLsnr = LogListener
            .matches("Could not log class path entry")
            .build();

        LogListener.Builder contenLsnrBuilder = LogListener.builder();

        String jarPath = new StringBuilder(javaHome)
            .append(javaHome.endsWith(File.separator) ? "" : File.separator)
            .append("lib")
            .append(File.separator)
            .toString();

        Iterable<Path> jars = Files.newDirectoryStream(Paths.get(jarPath), "*.jar");

        for (Path jar : jars)
            contenLsnrBuilder.andMatches(jar.getFileName().toString());

        Arrays.stream(javaClassPath.split(File.separator))
            .filter(fileName -> new File(fileName).isDirectory())
            .forEach(contenLsnrBuilder::andMatches);

        LogListener contentLsnr = contenLsnrBuilder.build();

        listeningLog.registerListener(lsnr);
        listeningLog.registerListener(clsPathValuelsnr);
        listeningLog.registerListener(errLsnr);
        listeningLog.registerListener(contentLsnr);

        startGrid(0);

        assertTrue(lsnr.check());
        assertTrue(clsPathValuelsnr.check());
        assertTrue(contentLsnr.check());

        assertFalse(errLsnr.check());
    }
}
