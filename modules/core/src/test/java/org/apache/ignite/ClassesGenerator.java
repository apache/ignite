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

package org.apache.ignite;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;

/**
 * Serialized classes generator.
 */
public class ClassesGenerator {
    /** */
    private static final String PATH = "modules/core/src/main/java/org/apache/ignite/internal/classnames.properties";

    /** */
    private static final String HEADER =
        "#\n" +
        "# Licensed to the Apache Software Foundation (ASF) under one or more\n" +
        "# contributor license agreements.  See the NOTICE file distributed with\n" +
        "# this work for additional information regarding copyright ownership.\n" +
        "# The ASF licenses this file to You under the Apache License, Version 2.0\n" +
        "# (the \"License\"); you may not use this file except in compliance with\n" +
        "# the License.  You may obtain a copy of the License at\n" +
        "#\n" +
        "#      http://www.apache.org/licenses/LICENSE-2.0\n" +
        "#\n" +
        "# Unless required by applicable law or agreed to in writing, software\n" +
        "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        "# See the License for the specific language governing permissions and\n" +
        "# limitations under the License.\n" +
        "#";

    /** */
    private static final String[] INCLUDED_PACKAGES = {
        "org.apache.ignite",
        "org.jdk8.backport",
        "org.pcollections",
        "com.romix.scala",
        "java.lang",
        "java.util",
        "java.net"
    };

    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(new File(U.getIgniteHome(), PATH));

        out.println(HEADER);
        out.println();

        for (Class cls : classes())
            out.println(cls.getName());
    }

    /**
     * @return Classes.
     * @throws Exception In case of error.
     */
    private static Collection<Class> classes() throws Exception {
        Collection<Class> col = new TreeSet<>(new Comparator<Class>() {
            @Override public int compare(Class c1, Class c2) {
                return c1.getName().compareTo(c2.getName());
            }
        });

        URLClassLoader ldr = (URLClassLoader)ClassesGenerator.class.getClassLoader();

        for (URL url : ldr.getURLs()) {
            File file = new File(url.toURI());

            int prefixLen = file.getPath().length() + 1;

            processFile(file, ldr, prefixLen, col);
        }

        return col;
    }

    /**
     * @param file File.
     * @param ldr Class loader.
     * @param prefixLen Prefix length.
     * @param col Classes.
     * @throws Exception In case of error.
     */
    private static void processFile(File file, ClassLoader ldr, int prefixLen, Collection<Class> col) throws Exception {
        if (!file.exists())
            throw new FileNotFoundException("File doesn't exist: " + file);

        if (file.isDirectory()) {
            for (File f : file.listFiles())
                processFile(f, ldr, prefixLen, col);
        }
        else {
            assert file.isFile();

            String path = file.getPath();

            if (path.toLowerCase().endsWith(".jar")) {
                try (JarInputStream jin = new JarInputStream(new BufferedInputStream(new FileInputStream(path)))) {
                    JarEntry entry;

                    while ((entry = jin.getNextJarEntry()) != null) {
                        if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".class"))
                            processClassFile(entry.getName(), ldr, 0, col);
                    }
                }
            }
            else if (path.toLowerCase().endsWith(".class"))
                processClassFile(path, ldr, prefixLen, col);
        }
    }

    /**
     * @param path File path.
     * @param ldr Class loader.
     * @param prefixLen Prefix length.
     * @param col Classes.
     * @throws Exception In case of error.
     */
    private static void processClassFile(String path, ClassLoader ldr, int prefixLen, Collection<Class> col)
        throws Exception {
        String clsName = path.substring(prefixLen, path.length() - 6).replace(File.separatorChar, '.');

        boolean included = false;

        for (String pkg : INCLUDED_PACKAGES) {
            if (clsName.startsWith(pkg)) {
                included = true;

                break;
            }
        }

        if (included) {
            Class<?> cls = Class.forName(clsName, false, ldr);

            if (!cls.isInterface() && !Modifier.isAbstract(cls.getModifiers()) &&
                Serializable.class.isAssignableFrom(cls))
                col.add((Class)cls);
        }
    }
}
