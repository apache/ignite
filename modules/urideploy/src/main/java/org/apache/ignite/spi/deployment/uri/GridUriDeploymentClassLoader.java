/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

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
            catch (Throwable e) {
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
            catch (Throwable e) {
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
