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
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Factory class to create class loader that loads classes and resources from
 * the GAR file or "unpacked" GAR file (GAR directory).
 * <p>
 * Class loader scans GAR file or GAR directory first and than if
 * class/resource was not found scans all JAR files.
 * It is assumed that all libraries are in the {@link #DFLT_LIBS_DIR_PATH}
 * directory.
 */
class GridUriDeploymentClassLoaderFactory {
    /** Libraries directory default value (value is {@code lib}). */
    public static final String DFLT_LIBS_DIR_PATH = "lib";

    /**
     * @param parent Parent class loader.
     * @param file GAR file or directory with unpacked GAR file.
     * @param log Logger.
     * @return Class Loader.
     * @throws org.apache.ignite.spi.IgniteSpiException In case of any error.
     */
    public static ClassLoader create(ClassLoader parent, File file, IgniteLogger log) throws IgniteSpiException {
        assert parent != null;
        assert file != null;
        assert log != null;

        assert file.isDirectory();

        List<URL> urls = new ArrayList<>();

        try {
            String url = file.toURI().toURL().toString();

            URL mainUrl = url.length() > 0 && url.charAt(url.length() - 1) == '/' ?
                file.toURI().toURL() : new URL(url + '/');

            urls.add(mainUrl);

            File libDir = new File(file, DFLT_LIBS_DIR_PATH);

            if (libDir.exists()) {
                File[] files = libDir.listFiles(new FilenameFilter() {
                    @Override public boolean accept(File dir, String name) {
                        return name.endsWith(".jar");
                    }
                });

                if (files.length > 0) {
                    for (File jarFile : files) {
                        urls.add(jarFile.toURI().toURL());
                    }
                }
            }

            return new GridUriDeploymentClassLoader(urls.toArray(new URL[urls.size()]), parent);
        }
        catch (MalformedURLException e) {
            throw new IgniteSpiException("Failed to create class loader for GAR file: " + file, e);
        }
    }

    /**
     * Ensure singleton.
     */
    private GridUriDeploymentClassLoaderFactory() {
        // No-op.
    }
}