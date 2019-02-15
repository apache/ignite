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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Loads classes and resources from "unpacked" GAR file (GAR directory).
 * <p>
 * Class loader scans GAR directory first and then if
 * class/resource was not found scans all JAR files.
 */
class GridUriDeploymentClassLoader extends URLClassLoader {
    /**
     * Creates new instance of class loader.
     *
     * @param urls The URLs from which to load classes and resources.
     * @param parent The parent class loader for delegation.
     */
    GridUriDeploymentClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    /** {@inheritDoc} */
    @Override protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // First, check if the class has already been loaded.
        Class cls = findLoadedClass(name);

        if (cls == null) {
            try {
                try {
                    // Search classes in GAR file.
                    // NOTE: findClass(String) is not overridden since it is always called after
                    // findLoadedClass(String) in exclusive synchronization block.
                    cls = findClass(name);
                }
                catch (ClassNotFoundException ignored) {
                    // If still not found, then invoke parent class loader in order
                    // to find the class.
                    cls = super.loadClass(name, resolve);
                }
            }
            catch (ClassNotFoundException e) {
                throw e;
            }
            // Catch Throwable to secure against any errors resulted from
            // corrupted class definitions or other user errors.
            catch (Exception e) {
                throw new ClassNotFoundException("Failed to load class due to unexpected error: " + name, e);
            }
        }

        return cls;
    }

    /**
     * Load class from GAR file.
     *
     * @param name Class name.
     * @return Loaded class.
     * @throws ClassNotFoundException If no class found.
     */
    public synchronized Class<?> loadClassGarOnly(String name) throws ClassNotFoundException {
        // First, check if the class has already been loaded.
        Class<?> cls = findLoadedClass(name);

        if (cls == null) {
            try {
                // Search classes in GAR file.
                // NOTE: findClass(String) is not overridden since it is always called after
                // findLoadedClass(String) in exclusive synchronization block.
                cls = findClass(name);
            }
            catch (ClassNotFoundException e) {
                throw e;
            }
            // Catch Throwable to secure against any errors resulted from
            // corrupted class definitions or other user errors.
            catch (Exception e) {
                throw new ClassNotFoundException("Failed to load class due to unexpected error: " + name, e);
            }
        }

        return cls;
    }

    /** {@inheritDoc} */
    @Override public URL getResource(String name) {
        URL url = findResource(name);

        if (url == null)
            url = ClassLoader.getSystemResource(name);

        if (url == null)
            url = super.getResource(name);

        return url;
    }

    /** {@inheritDoc} */
    @Override public InputStream getResourceAsStream(String name) {
        // Find resource in GAR file first.
        InputStream in = getResourceAsStreamGarOnly(name);

        // Find resource in parent class loader.
        if (in == null)
            in = ClassLoader.getSystemResourceAsStream(name);

        if (in == null)
            in = super.getResourceAsStream(name);

        return in;
    }

    /**
     * Returns an input stream for reading the specified resource from GAR file only.
     *
     * @param name Resource name.
     * @return An input stream for reading the resource, or {@code null}
     *      if the resource could not be found.
     */
    @Nullable public InputStream getResourceAsStreamGarOnly(String name) {
        URL url = findResource(name);

        try {
            return url != null ? url.openStream() : null;
        }
        catch (IOException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentClassLoader.class, this, "urls", Arrays.toString(getURLs()));
    }
}