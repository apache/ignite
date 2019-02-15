/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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