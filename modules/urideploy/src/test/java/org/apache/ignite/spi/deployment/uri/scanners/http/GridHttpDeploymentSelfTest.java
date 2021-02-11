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

package org.apache.ignite.spi.deployment.uri.scanners.http;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentAbstractSelfTest;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.junit.Test;

/**
 * Test http scanner.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridHttpDeploymentSelfTest extends GridUriDeploymentAbstractSelfTest {
    /** Frequency */
    private static final int FREQ = 5000;

    /** */
    public static final String LIBS_GAR = "libs-file.gar";

    /** */
    public static final String CLASSES_GAR = "classes-file.gar";

    /** */
    public static final String ALL_GAR = "file.gar";

    /** */
    public static final String ALL_JAR = "file.jar";

    /** Gar-file which contains libs. */
    public static final String LIBS_GAR_FILE_PATH = U.resolveIgnitePath(
        GridTestProperties.getProperty("ant.urideployment.gar.libs-file")).getPath();

    /** Gar-file which contains classes (cannot be used without libs). */
    public static final String CLASSES_GAR_FILE_PATH = U.resolveIgnitePath(
        GridTestProperties.getProperty("ant.urideployment.gar.classes-file")).getPath();

    /** Gar-file which caontains both libs and classes. */
    public static final String ALL_GAR_FILE_PATH = U.resolveIgnitePath(
        GridTestProperties.getProperty("ant.urideployment.gar.file")).getPath();

    /** JAR file with tasks and dependencies. */
    public static final String JAR_FILE_PATH = U.resolveIgnitePath(
            GridTestProperties.getProperty("urideployment.jar.uri")).getPath();

    /** Jetty. */
    private static Server srv;

    /** Resource base. */
    private static String rsrcBase;

    /** {@inheritDoc} */
    @Override protected void beforeSpiStarted() throws Exception {
        srv = new Server();

        ServerConnector conn = new ServerConnector(srv);

        conn.setPort(8080);

        srv.addConnector(conn);

        ResourceHandler hnd = new ResourceHandler();

        hnd.setDirectoriesListed(true);

        String garPathTmp = GridTestProperties.getProperty("urideployment.path.tmp");

        File resourseBaseDir = U.resolveIgnitePath(garPathTmp);

        if (resourseBaseDir == null) {
            resourseBaseDir = new File(U.getIgniteHome(), garPathTmp);

            assertTrue(resourseBaseDir.mkdirs());
        }

        rsrcBase = resourseBaseDir.getPath();

        hnd.setResourceBase(rsrcBase);

        srv.setHandler(hnd);

        srv.start();

        assert srv.isStarted();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        assert srv.isStarted();

        srv.stop();

        assert srv.isStopped();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployUndeploy2GarFiles() throws Exception {
        String taskName = "org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3";

        checkNoTask(taskName);

        try {
            copyToResourceBase(LIBS_GAR_FILE_PATH, LIBS_GAR);

            copyToResourceBase(CLASSES_GAR_FILE_PATH, CLASSES_GAR);

            waitForTask(taskName, true, FREQ + 3000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            deleteFromResourceBase(LIBS_GAR);
            deleteFromResourceBase(CLASSES_GAR);

            waitForTask(taskName, false, FREQ + 3000);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployUndeployJar() throws Exception {
        String taskName = "org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask8";

        checkNoTask(taskName);

        try {
            copyToResourceBase(JAR_FILE_PATH, ALL_JAR);

            waitForTask(taskName, true, FREQ + 3000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            deleteFromResourceBase(ALL_JAR);

            waitForTask(taskName, false, FREQ + 3000);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSameContantFiles() throws Exception {
        String taskName = "org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3";

        checkNoTask(taskName);

        try {
            copyToResourceBase(ALL_GAR_FILE_PATH, ALL_GAR);

            waitForTask(taskName, true, FREQ + 3000);

            copyToResourceBase(ALL_GAR_FILE_PATH, "file-copy.gar");

            waitForTask(taskName, true, FREQ + 3000);
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
        finally {
            deleteFromResourceBase(ALL_GAR);
            deleteFromResourceBase("file-copy.gar");

            waitForTask(taskName, false, FREQ + 3000);
        }
    }

    /**
     * @return Test server URl as deployment source URI.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList("http://freq=" + FREQ + "@localhost:8080/");
    }

    /**
     * @param fileName File name.
     */
    private void deleteFromResourceBase(String fileName) {
        File file = new File(rsrcBase + '/' + fileName);

        if (!file.delete())
            U.warn(log, "Could not delete file: " + file);
    }

    /**
     * @param path Path to the file which should be copied.
     * @param newFileName New file name.
     * @throws IOException If exception.
     */
    private void copyToResourceBase(String path, String newFileName) throws IOException {
        File file = new File(path);

        assert file.exists() : "Test file not found [path=" + path + ']';

        File newFile = new File(rsrcBase + '/' + newFileName);

        assert !newFile.exists();

        U.copy(file, newFile, false);

        assert newFile.exists();
    }
}
