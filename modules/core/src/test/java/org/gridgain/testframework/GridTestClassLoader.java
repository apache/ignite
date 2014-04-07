/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Test class loader.
 */
public class GridTestClassLoader extends ClassLoader {
    /** */
    private final Map<String, String> rsrcs;

    /** */
    private final String[] clsNames;

    /**
     * @param clsNames Test Class names.
     */
    public GridTestClassLoader(String... clsNames) {
        this(null, GridTestClassLoader.class.getClassLoader(), clsNames);
    }

    /**
     * @param clsNames Test Class name.
     * @param rsrcs Resources.
     */
    public GridTestClassLoader(Map<String, String> rsrcs, String... clsNames) {
        this(rsrcs, GridTestClassLoader.class.getClassLoader(), clsNames);
    }

    /**
     * @param clsNames Test Class name.
     * @param rsrcs Resources.
     * @param parent Parent class loader.
     */
    public GridTestClassLoader(@Nullable Map<String, String> rsrcs, ClassLoader parent, String... clsNames) {
        super(parent);

        this.rsrcs = rsrcs;
        this.clsNames = clsNames;
    }

    /** {@inheritDoc} */
    @Override protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> res = findLoadedClass(name);

        if (res != null)
            return res;

        boolean patch = false;

        for (String clsName : clsNames)
            if (name.equals(clsName))
                patch = true;

        if (patch) {
            String path = name.replaceAll("\\.", "/") + ".class";

            InputStream in = getResourceAsStream(path);

            if (in != null) {
                GridByteArrayList bytes = new GridByteArrayList(1024);

                try {
                    bytes.readAll(in);
                }
                catch (IOException e) {
                    throw new ClassNotFoundException("Failed to upload class ", e);
                }

                return defineClass(name, bytes.internalArray(), 0, bytes.size());
            }

            throw new ClassNotFoundException("Failed to upload resource [class=" + path + ", parent classloader="
                + getParent() + ']');
        }

        // Maybe super knows.
        return super.loadClass(name, resolve);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public InputStream getResourceAsStream(String name) {
        if (rsrcs != null && rsrcs.containsKey(name))
            return new StringBufferInputStream(rsrcs.get(name));

        return getParent().getResourceAsStream(name);
    }
}
