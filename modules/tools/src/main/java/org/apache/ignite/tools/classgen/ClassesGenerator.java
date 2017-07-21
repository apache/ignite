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

package org.apache.ignite.tools.classgen;

import java.io.BufferedInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Serialized classes generator.
 */
public class ClassesGenerator {
    /** */
    private static final String META_INF = "META-INF/";

    /** */
    private static final String DEFAULT_FILE_PATH = META_INF + "classnames.properties";

    /** */
    private static final String[] EXCLUDED_PACKAGES = {
        "org.apache.ignite.tools"
    };

    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        assert args.length >= 3;

        String basePath = args[0];
        String hdr = args[1];
        String[] packages = args[2].split(":");
        String finaName = args.length == 4 ? args[3] : null;

        ClassesGenerator gen = new ClassesGenerator(basePath, hdr, packages, finaName);

        gen.generate();
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

    /** */
    private final String basePath;

    /** */
    private final String hdr;

    /** */
    private final String[] packages;

    /** */
    private final String fileName;

    /**
     * @param basePath Base file path.
     * @param hdr Header.
     * @param packages Included packages.
     * @param fileName Property file name.
     */
    private ClassesGenerator(String basePath, String hdr, String[] packages, String fileName) {
        this.basePath = basePath;
        this.hdr = hdr;
        this.packages = packages;
        this.fileName = fileName;
    }

    /**
     * @throws Exception In case of error.
     */
    private void generate() throws Exception {
        System.out.println("Generating classnames.properties...");

        for (URL url : ldr.getURLs())
            processUrl(url);

        if (!errs.isEmpty()) {
            StringBuilder sb = new StringBuilder("Failed to generate classnames.properties due to errors:\n");

            for (String err : errs)
                sb.append("    ").append(err).append('\n');

            String msg = sb.toString().trim();

            System.out.println(msg);

            throw new Exception(msg);
        }

        PrintStream out = new PrintStream(new File(basePath,
            (fileName == null || fileName.isEmpty()) ? DEFAULT_FILE_PATH : META_INF + fileName));

        out.println(hdr);
        out.println();

        for (Class cls : classes)
            out.println(cls.getName());
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

        for (String pkg : EXCLUDED_PACKAGES) {
            if (clsName.startsWith(pkg))
                return;
        }

        boolean included = false;

        for (String pkg : packages) {
            if (clsName.startsWith(pkg)) {
                included = true;

                break;
            }
        }

        if (included) {
            Class<?> cls = Class.forName(clsName, false, ldr);

            if (Serializable.class.isAssignableFrom(cls) &&
                !(cls.getName().endsWith("Future") || cls.getName().endsWith("FutureAdapter"))) {
                if (!cls.isInterface() && !Modifier.isAbstract(cls.getModifiers()) && !cls.isEnum() &&
                    !cls.getSimpleName().isEmpty()) {
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

                    if (Externalizable.class.isAssignableFrom(cls)) {
                        try {
                            Constructor<?> cons = cls.getConstructor();

                            if (!Modifier.isPublic(cons.getModifiers()))
                                errs.add("Default constructor in Externalizable class is not public: " + cls.getName());
                        }
                        catch (NoSuchMethodException ignored) {
                            errs.add("No default constructor in Externalizable class: " + cls.getName());
                        }
                    }
                }

                classes.add((Class)cls);
            }
        }
    }
}
