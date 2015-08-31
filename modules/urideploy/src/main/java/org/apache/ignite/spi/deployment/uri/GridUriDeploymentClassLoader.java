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