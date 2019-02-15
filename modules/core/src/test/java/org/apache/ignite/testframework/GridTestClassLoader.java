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