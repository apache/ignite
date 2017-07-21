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

package org.apache.ignite.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.util.antgar.IgniteDeploymentGarAntTask;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;

/**
 * Tests for Ant task generating GAR file.
 */
@GridCommonTest(group = "Tools")
public class GridToolsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    public void testCorrectAntGarTask() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_0";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";
        String garDescDirName =
            U.resolveIgnitePath(GridTestProperties.getProperty("ant.gar.descriptor.dir")).getAbsolutePath()
            + File.separator + "ignite.xml";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Make Gar file
        U.copy(new File(garDescDirName), new File(metaDirName + File.separator + "ignite.xml"), true);

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        garTask.execute();

        File garFile = new File(garFileName);

        assert garFile.exists();

        boolean res = checkStructure(garFile, true);

        assert res;
    }

    /**
     * @param garFile GAR file.
     * @param hasDescr Whether GAR file has descriptor.
     * @throws IOException If GAR file is not found.
     * @return Whether GAR file structure is correct.
     */
    private boolean checkStructure(File garFile, boolean hasDescr) throws IOException {
        ZipFile zip = new ZipFile(garFile);

        String descr = "META-INF/ignite.xml";

        ZipEntry entry = zip.getEntry(descr);

        if (entry == null && !hasDescr) {
            if (log().isInfoEnabled()) {
                log().info("Process deployment without descriptor file [path=" +
                        descr + ", file=" + garFile.getAbsolutePath() + ']');
            }

            return true;
        }
        else if (entry != null && hasDescr) {
            InputStream in = null;

            try {
                in = new BufferedInputStream(zip.getInputStream(entry));

                return true;
            }
            finally {
                U.close(in, log());
            }
        }
        else
            return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAntGarTaskWithExternalP2PDescriptor() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_1";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";
        String garDescrDirName =
            U.resolveIgnitePath(GridTestProperties.getProperty("ant.gar.descriptor.dir")).getAbsolutePath();

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setDescrdir(new File(garDescrDirName));
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        garTask.execute();

        File garFile = new File(garFileName);

        assert garFile.exists();

        boolean res = checkStructure(garFile, true);

        assert res;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAntGarTaskWithDoubleP2PDescriptor() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_2";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";
        String garDescrDirName =
            U.resolveIgnitePath(GridTestProperties.getProperty("ant.gar.descriptor.dir")).getAbsolutePath()
            + File.separator + "ignite.xml";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Make Gar file
        U.copy(new File(garDescrDirName), new File(metaDirName + File.separator + "ignite.xml"), true);

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setDescrdir(new File(garDescrDirName));
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        try {
            garTask.execute();

            assert false;
        }
        catch (BuildException e) {
            if (log().isInfoEnabled())
                log().info(e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAntGarTaskWithDirDescriptor() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_3";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);
        garTask.setDescrdir(new File(garFileName));

        try {
            garTask.execute();

            assert false;
        }
        catch (BuildException e) {
            if (log().isInfoEnabled())
                log().info(e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    public void testAntGarTaskWithNullDescriptor() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_4";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        try {
            garTask.execute();
        }
        catch (BuildException e) {
            if (log().isInfoEnabled())
                log().info(e.getMessage());

            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAntGarTaskWithFileBaseDir() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_5";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setBasedir(new File(garFileName));
        garTask.setProject(garProject);
        garTask.setDescrdir(new File(garFileName));

        try {
            garTask.execute();

            assert false;
        }
        catch (BuildException e) {
            if (log().isInfoEnabled())
                log().info(e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAntGarTaskToString() throws Exception {
        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis() + "_6";
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garFileName = baseDirName + ".gar";

        // Make base and META-INF dir.
        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(new File(garFileName));
        garTask.setBasedir(new File(garFileName));
        garTask.setProject(garProject);
        garTask.setDescrdir(new File(garFileName));

        garTask.toString();
    }
}