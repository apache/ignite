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

package org.apache.ignite.util.antgar;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.Zip;
import org.apache.tools.ant.types.ArchiveFileSet;
import org.apache.tools.zip.ZipOutputStream;

/**
 * Ant task for generating GAR file. This task extends standard {@code zip} Ant task and
 * has two parameters:
 * <ul>
 * <li>{@code basedir} - Base directory for GAR archive.</li>
 * <li>
 *      {@code descrdir} - Directory where descriptor {@link #DESC_NAME} file is located.
 *      If not specified, it is assumed that Ignite descriptor will be searched in base directory
 *      (see {@link #setBasedir(File)}). <b>Note</b> further that GAR descriptor file is fully optional
 *      itself for GAR archive.
 * </li>
 * </ul>
 */
public class IgniteDeploymentGarAntTask extends Zip {
    /** GAR descriptor name. Its value is {@code ignite.xml}. */
    public static final String DESC_NAME = "ignite.xml";

    /**  Default descriptor path. */
    private static final String DESC_PATH = "META-INF";

    /** Descriptor directory. */
    private File descDir;

    /** Descriptor file name. */
    private File descFile;

    /** Base directory of Ant task. */
    private File baseDir;

    /**
     * Creates ant task with default values.
     */
    public IgniteDeploymentGarAntTask() {
        archiveType = "gar";
        emptyBehavior = "create";
    }

    /**
     * Sets the directory where descriptor is located. This parameter is optional and if not set Ant task
     * will search for descriptor file in base directory. <b>Note</b> further that GAR descriptor file is fully optional
     * itself for GAR archive.
     *
     * @param descDir Descriptor directory.
     */
    public void setDescrdir(File descDir) {
        assert descDir != null;

        this.descDir = descDir;
    }

    /**
     * Sets base directory for the archive.
     *
     * @param baseDir Base archive directory to set.
     */
    @Override public void setBasedir(File baseDir) {
        super.setBasedir(baseDir);

        this.baseDir = baseDir;
    }

    /**
     * Executes the Ant task.
     */
    @Override public void execute() {
        setEncoding("UTF8");

        // Otherwise super method will throw exception.
        if (baseDir != null && baseDir.isDirectory()) {

            File[] files = baseDir.listFiles(new FileFilter() {
                /** {@inheritDoc} */
                @Override public boolean accept(File pathname) {
                    return pathname.isDirectory() && pathname.getName().equals(DESC_PATH);
                }
            });

            if (files.length == 1) {
                files = files[0].listFiles(new FileFilter() {
                    /** {@inheritDoc} */
                    @Override public boolean accept(File pathname) {
                        return !pathname.isDirectory() && pathname.getName().equals(DESC_NAME);
                    }
                });
            }

            File desc = null;

            if (files.length == 1)
                desc = files[0];

            // File was defined in source.
            if (desc != null) {
                if (descDir != null) {
                    throw new BuildException("Ignite descriptor '" + DESC_NAME + "' is already " +
                        "defined in source folder.");
                }
            }
            // File wasn't defined in source and could be defined using 'descrdir' attribute.
            // Try to find the descriptor in defined directory.
            else if (descDir != null) {
                if (!descDir.isDirectory()) {
                    throw new BuildException(
                        "'descrdir' attribute isn't folder [dir=" + descDir.getAbsolutePath() + ']');
                }

                descFile = new File(getFullPath(descDir.getAbsolutePath(), DESC_NAME));

                if (!descFile.exists()) {
                    throw new BuildException("Folder doesn't contain Ignite descriptor [path=" +
                            descDir.getAbsolutePath() + ']');
                }
            }
        }

        super.execute();
    }

    /** {@inheritDoc} */
    @Override protected void initZipOutputStream(ZipOutputStream zOut) throws IOException {
        if (descFile != null)
            zipFile(descFile, zOut, getFullPath(DESC_PATH, DESC_NAME), ArchiveFileSet.DEFAULT_FILE_MODE);
    }

    /**
     * Constructs full path given two other paths.
     *
     * @param subPath1 1st path.
     * @param subPath2 2nd path.
     * @return Full path.
     */
    private static String getFullPath(String subPath1, String subPath2) {
        assert subPath1 != null;
        assert subPath2 != null;

        char c = subPath1.charAt(subPath1.length() - 1);

        boolean b1 = c == '/' || c == '\\';

        c = subPath2.charAt(0);

        boolean b2 = c == '/' || c == '\\';

        return b1 != b2 ? subPath1 + subPath2 : !b1 ? subPath1 + '/' + subPath2 :
            subPath1.substring(0, subPath1.length() - 1) + File.separatorChar + subPath2.substring(1);
    }
}