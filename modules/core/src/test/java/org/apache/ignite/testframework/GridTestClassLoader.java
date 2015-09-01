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

package org.apache.ignite.testframework;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.util.Map;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.jetbrains.annotations.Nullable;

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