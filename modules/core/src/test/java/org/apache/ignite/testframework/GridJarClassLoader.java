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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.CodeSource;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Jar class loader.
 */
public final class GridJarClassLoader extends SecureClassLoader {
    /** Cached loaded classes as bytes. */
    private final Map<String, byte[]> clsArrs;

    /** List of excluded classes/packages. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private List<String> excludedCls = new ArrayList<>();

    /** */
    private static GridJarClassLoader instance;

    /**
     * Get classloader singleton instance.
     *
     * @param files Files.
     * @param parent Parent classloader.
     * @return Instance of Jar class loader.
     * @throws IOException If fies can't be read,
     */
    public static synchronized GridJarClassLoader getInstance(List<String> files, ClassLoader parent)
        throws IOException{
        if (instance == null)
            instance = new GridJarClassLoader(files, parent);

        return instance;
    }

    /**
     * Constructor.
     *
     * @param files Files.
     * @param parent Parent classloader.
     * @throws IOException If fies can't be read,
     */
    private GridJarClassLoader(Iterable<String> files, ClassLoader parent) throws IOException {
        super(parent);

        clsArrs = new HashMap<>();

        for (String fileName: files)
            readJarFile(fileName);
    }

    /** {@inheritDoc} */
    @Override protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        boolean excluded = false;

        for (String cls: excludedCls)
            if (name.startsWith(cls)) {
                excluded = true;

                break;
            }

        // If class is from Jar file(s) and not in excluded (note we use name with '.').
        if (clsArrs.containsKey(name) && !excluded) {
            Class<?> cls = findLoadedClass(name);

            if (cls == null)
                cls = findClass(name);

            if (resolve)
                resolveClass(cls);

            return cls;
        }

        return super.loadClass(name, resolve);
    }


    /** {@inheritDoc} */
    @Override protected Class<?> findClass(String name) throws ClassNotFoundException {

        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            int i = name.lastIndexOf('.');

            if (i >= 0)
                sm.checkPackageDefinition(name.substring(0, i));
        }

        byte[] buf = clsArrs.get(name);

        if (buf != null)
            return defineClass(name, buf, 0, buf.length, (CodeSource)null);

        throw new ClassNotFoundException(name);
    }

    /**
     * Reads JAR file and stored classes locally.
     *
     * @param fileName Name of file to read.
     * @throws IOException If read failed.
     */
    private void readJarFile(String fileName) throws IOException {
        JarEntry je;

        JarInputStream jis = new JarInputStream(new FileInputStream(fileName));

        while ((je = jis.getNextJarEntry()) != null) {
            String jarName = je.getName();

            if (jarName.endsWith(".class"))
                loadClassBytes(jis, jarName);

            // Else ignore it; it could be an image or audio file.
            jis.closeEntry();
        }
    }

    /**
     * Loads class bytes to storege.
     *
     * @param jis Input stream.
     * @param jarName Name of the JAR file.
     * @throws IOException If read failed.
     */
    private void loadClassBytes(JarInputStream jis, String jarName)  throws IOException {
        BufferedInputStream jarBuf = new BufferedInputStream(jis);
        ByteArrayOutputStream jarOut = new ByteArrayOutputStream();

        int b;

        while ((b = jarBuf.read()) != -1)
            jarOut.write(b);

        // Remove ".class".
        String urlName = jarName.substring(0, jarName.length() - 6);

        String name = urlName.replace('/', '.');

        clsArrs.put(name, jarOut.toByteArray());
    }

    /**
     * @return the excludedCls
     */
    public List<String> getExcludedCls() {
        return excludedCls;
    }

    /**
     * @param excludedCls the excludedCls to set
     */
    public void setExcludedCls(List<String> excludedCls) {
        this.excludedCls = excludedCls;
    }
}