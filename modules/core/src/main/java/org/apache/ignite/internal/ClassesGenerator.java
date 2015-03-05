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

package org.apache.ignite.internal;

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
    private static final String DFLT_BASE_PATH = U.getIgniteHome() + "/modules/core/src/main/java";

    /** */
    private static final String FILE_PATH = "org/apache/ignite/internal/classnames.properties";

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
        String basePath = args.length > 0 ? args[0] : DFLT_BASE_PATH;

        File file = new File(basePath, FILE_PATH);

        ClassesGenerator gen = new ClassesGenerator();

        write(file, gen.generate());
    }

    /**
     * @param file File.
     * @param classes Classes.
     * @throws Exception In case of error.
     */
    private static void write(File file, Collection<Class> classes) throws Exception {
        PrintStream out = new PrintStream(file);

        out.println(HEADER);
        out.println();

        for (Class cls : classes)
            out.println(cls.getName());
    }

    /** */
    private final URLClassLoader ldr = (URLClassLoader)getClass().getClassLoader();

    /** */
    private final Collection<Class> classes = new TreeSet<>(new Comparator<Class>() {
        @Override public int compare(Class c1, Class c2) {
            return c1.getName().compareTo(c2.getName());
        }
    });

    /** */
    private final Collection<String> errs = new ArrayList<>();

    /**
     * @throws Exception In case of error.
     * @return Classes.
     */
    private Collection<Class> generate() throws Exception {
        System.out.println("Generating classnames.properties...");

        URLClassLoader ldr0 = ldr;

        while (ldr0 != null) {
            for (URL url : ldr0.getURLs())
                processUrl(url);

            ldr0 = (URLClassLoader)ldr0.getParent();
        }

        if (!errs.isEmpty()) {
            StringBuilder sb = new StringBuilder("Failed to generate classnames.properties due to errors:\n");

            for (String err : errs)
                sb.append("    ").append(err).append('\n');

            throw new Exception(sb.toString().trim());
        }

        classes.add(byte[].class);
        classes.add(short[].class);
        classes.add(int[].class);
        classes.add(long[].class);
        classes.add(float[].class);
        classes.add(double[].class);
        classes.add(char[].class);
        classes.add(boolean[].class);
        classes.add(Object[].class);

        return classes;
    }

    /**
     * @param url URL.
     * @throws Exception In case of error.
     */
    private void processUrl(URL url) throws Exception {
        System.out.println("    Processing URL: " + url);

        File file = new File(url.toURI());

        int prefixLen = file.getPath().length() + 1;

        processFile(file, prefixLen);
    }

    /**
     * @param file File.
     * @throws Exception In case of error.
     */
    private void processFile(File file, int prefixLen) throws Exception {
        if (!file.exists())
            throw new FileNotFoundException("File doesn't exist: " + file);

        if (file.isDirectory()) {
            for (File f : file.listFiles())
                processFile(f, prefixLen);
        }
        else {
            assert file.isFile();

            String path = file.getPath();

            if (path.toLowerCase().endsWith(".jar")) {
                try (JarInputStream jin = new JarInputStream(new BufferedInputStream(new FileInputStream(path)))) {
                    JarEntry entry;

                    while ((entry = jin.getNextJarEntry()) != null) {
                        if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".class"))
                            processClassFile(entry.getName(), 0);
                    }
                }
            }
            else if (path.toLowerCase().endsWith(".class"))
                processClassFile(path, prefixLen);
        }
    }

    /**
     * @param path File path.
     * @param prefixLen Prefix length.
     * @throws Exception In case of error.
     */
    private void processClassFile(String path, int prefixLen)
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

            boolean isSerializable = !cls.isInterface() && !Modifier.isAbstract(cls.getModifiers()) &&
                Serializable.class.isAssignableFrom(cls);

            if (isSerializable) {
                if (!cls.isEnum() && !cls.getSimpleName().isEmpty() && cls.getName().startsWith("org.apache.ignite")) {
                    try {
                        Field field = cls.getDeclaredField("serialVersionUID");

                        if (!field.getType().equals(long.class))
                            errs.add("serialVersionUID field is not long in class: " + cls.getName());

                        int mod = field.getModifiers();

                        if (!Modifier.isStatic(mod))
                            errs.add("serialVersionUID field is not static in class: " + cls.getName());

                        if (!Modifier.isFinal(mod))
                            errs.add("serialVersionUID field is not final in class: " + cls.getName());
                    }
                    catch (NoSuchFieldException ignored) {
                        errs.add("No serialVersionUID field in class: " + cls.getName());
                    }
                }

                classes.add((Class)cls);
            }
        }
    }
}
