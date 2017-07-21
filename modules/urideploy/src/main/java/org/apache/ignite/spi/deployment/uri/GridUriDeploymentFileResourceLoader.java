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
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Class loader helper that could load class from the file using certain
 * class loader.
 */
class GridUriDeploymentFileResourceLoader {
    /** Class loader class should be loaded by. */
    private final ClassLoader clsLdr;

    /** Initial directory. */
    private final File scanPathDir;

    /**
     * Creates new instance of loader helper.
     *
     * @param clsLdr Class loader class should be loaded by.
     * @param scanPathDir Initial directory.
     */
    GridUriDeploymentFileResourceLoader(ClassLoader clsLdr, File scanPathDir) {
        this.clsLdr = clsLdr;
        this.scanPathDir = scanPathDir;
    }

    /**
     * Creates new class from file with given file name.
     *
     * @param fileName Name of the class to be loaded. It might be either
     *      fully-qualified or just a class name.
     * @param ignoreUnknownRsrc Whether unresolved classes should be
     *      ignored or not.
     * @return Loaded class.
     * @throws org.apache.ignite.spi.IgniteSpiException If class could not be loaded and
     *      {@code ignoreUnknownRsrc} parameter is {@code true}.
     */
    @SuppressWarnings("unchecked")
    Class<? extends ComputeTask<?, ?>> createResource(String fileName, boolean ignoreUnknownRsrc) throws IgniteSpiException {
        if (scanPathDir.isDirectory())
            fileName = fileName.substring(scanPathDir.getAbsolutePath().length() + 1);

        if (fileName.endsWith(".class")) {
            String str = fileName;

            // Replace separators.
            str = str.replaceAll("\\/|\\\\", ".");

            // Strip off '.class' extension.
            str = str.substring(0, str.indexOf(".class"));

            try {
                return (Class<? extends ComputeTask<?, ?>>)clsLdr.loadClass(str);
            }
            catch (ClassNotFoundException e) {
                if (ignoreUnknownRsrc) {
                    // No-op.
                }
                else
                    throw new IgniteSpiException("Failed to load class: " + str, e);
            }
        }

        // Not a class resource.
        return null;
    }
}