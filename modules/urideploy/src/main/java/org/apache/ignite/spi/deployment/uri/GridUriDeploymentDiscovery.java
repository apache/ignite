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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Helper that loads classes either from directory or from JAR file.
 * <p>
 * If loading from directory is used, helper scans given directory
 * and all subdirectories recursively and loads all files
 * with ".class" extension by given class loader. If class could not
 * be loaded it will be ignored.
 * <p>
 * If JAR file loading is used helper scans JAR file and tries to
 * load all {@link JarEntry} assuming it's a file name.
 * If at least one of them could not be loaded helper fails.
 */
final class GridUriDeploymentDiscovery {
    /**
     * Enforces singleton.
     */
    private GridUriDeploymentDiscovery() {
        // No-op.
    }

    /**
     * Load classes from given file. File could be either directory or JAR file.
     *
     * @param clsLdr Class loader to load files.
     * @param file Either directory or JAR file which contains classes or
     *      references to them.
     * @return Set of found and loaded classes or empty set if file does not
     *      exist.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if given JAR file references to none
     *      existed class or IOException occurred during processing.
     */
    static Set<Class<? extends ComputeTask<?, ?>>> getClasses(ClassLoader clsLdr, File file)
        throws IgniteSpiException {
        Set<Class<? extends ComputeTask<?, ?>>> rsrcs = new HashSet<>();

        if (file.exists() == false)
            return rsrcs;

        GridUriDeploymentFileResourceLoader fileRsrcLdr = new GridUriDeploymentFileResourceLoader(clsLdr, file);

        if (file.isDirectory())
            findResourcesInDirectory(fileRsrcLdr, file, rsrcs);
        else {
            try {
                for (JarEntry entry : U.asIterable(new JarFile(file.getAbsolutePath()).entries())) {
                    Class<? extends ComputeTask<?, ?>> rsrc = fileRsrcLdr.createResource(entry.getName(), false);

                    if (rsrc != null)
                        rsrcs.add(rsrc);
                }
            }
            catch (IOException e) {
                throw new IgniteSpiException("Failed to discover classes in file: " + file.getAbsolutePath(), e);
            }
        }

        return rsrcs;
    }

    /**
     * Recursively scans given directory and load all found files by loader.
     *
     * @param clsLdr Loader that could load class from given file.
     * @param dir Directory which should be scanned.
     * @param rsrcs Set which will be filled in.
     */
    @SuppressWarnings({"UnusedCatchParameter"})
    private static void findResourcesInDirectory(GridUriDeploymentFileResourceLoader clsLdr, File dir,
        Set<Class<? extends ComputeTask<?, ?>>> rsrcs) {
        assert dir.isDirectory() == true;

        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                // Recurse down into directories.
                findResourcesInDirectory(clsLdr, file, rsrcs);
            }
            else {
                Class<? extends ComputeTask<?, ?>> rsrc = null;

                try {
                    rsrc = clsLdr.createResource(file.getAbsolutePath(), true);
                }
                catch (IgniteSpiException e) {
                    // Must never happen because we use 'ignoreUnknownRsrc=true'.
                    assert false;
                }

                if (rsrc != null)
                    rsrcs.add(rsrc);
            }
        }
    }
}