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

package org.apache.ignite.tools.marshaller.optimized;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;

/**
 * Generates a file with preregistered class names.
 * <p>
 * This class should have the following modules with corresponding lib dependencies in its classpath:
 * <ul>
 * <li>src</li>
 * <li>scalar</li>
 * <li>visor</li>
 * </ul>
 * It {@code should NOT} have any {@code 'test'} modules in the class path.
 */
public class OptimizedClassNamesGenerator {
    /** Defines Ignite installation folder. */
    public static final String IGNITE_HOME_SYS_PROP = "IGNITE_HOME";

    /** Defines Ignite installation folder.  */
    public static final String IGNITE_HOME_ENV_VAR = "IGNITE_HOME";

    /** File name to generate. */
    public static final String FILE_NAME = "optimized-classnames.properties";

    /** Previous version file name. */
    public static final String PREV_FILE_NAME = "optimized-classnames.previous.properties";

    /** */
    private Collection<String> clsNames;

    /** Collection of classes without serialVersionUID. */
    private Collection<Class> clsWithoutSerialVersionUID;

    /** */
    private int urlPrefixLen;

    /**
     * @param file Output file.
     * @param prev Previous version file.
     * @throws URISyntaxException If this URL is not formatted strictly according to
     * to RFC2396 and cannot be converted to a URI.
     * @throws IOException If an I/O error occurs while writing stream header.
     */
    public void writeClassNames(File file, File prev) throws IOException, URISyntaxException {
        assert file != null;
        assert prev != null;

        clsNames = new HashSet<>();

        clsWithoutSerialVersionUID = new ArrayList<>();

        URL[] urls = ((URLClassLoader)OptimizedClassNamesGenerator.class.getClassLoader()).getURLs();

        System.out.println("Printing class loader URLs: ");

        for (URL url : urls)
            System.out.println("  " + url);

        for (URL url : urls) {
            File f = new File(url.toURI());

            if (!f.exists())
                System.err.println("Unrecognized URL: " + url);
            else {
                urlPrefixLen = f.getPath().length() + 1;

                processFile(f);
            }
        }

        if (clsWithoutSerialVersionUID.size() > 0) {
            StringBuilder sb = new StringBuilder("No serialVersionUID field in class(es): ");

            for (Class cls : clsWithoutSerialVersionUID)
                sb.append(cls.getName()).append(", ");

            sb.setLength(sb.length() - ", ".length());

            throw new RuntimeException(sb.toString());
        }

        Collection<String> prevCls = previousVersionClasses(prev);

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
            for (String clsName : prevCls) {
                writer.write(clsName);
                writer.newLine();

                clsNames.remove(clsName);
            }

            List<String> sorted = new ArrayList<>(clsNames);

            Collections.sort(sorted);

            for (String clsName : sorted) {
                writer.write(clsName);
                writer.newLine();
            }
        }
    }

    /**
     * @param file Previous version file.
     * @return Class names.
     * @throws IOException In case of error.
     */
    private Collection<String> previousVersionClasses(File file) throws IOException {
        assert file != null && file.exists();

        BufferedReader rdr = new BufferedReader(new FileReader(file));

        Collection<String> names = new ArrayList<>(30000);

        String name;

        while ((name = rdr.readLine()) != null)
            names.add(name);

        return names;
    }

    /**
     * @param file File to process.
     * @throws IOException If an I/O error occurs while writing stream header.
     */
    private void processFile(File file) throws IOException {
        if (file.isDirectory())
            for (File childFile : file.listFiles())
                processFile(childFile);
        else if (file.getName().toLowerCase().endsWith(".jar"))
            processJar(file);
        else if (file.getName().toLowerCase().endsWith(".class"))
            processClass(trimClassExtension(
                file.getPath().substring(urlPrefixLen).replace(File.separatorChar, '.')
            ));
    }

    /**
     * @param jarFile File to process.
     * @throws IOException If an I/O error occurs while writing stream header.
     */
    private void processJar(File jarFile) throws IOException {
        JarInputStream jin = new JarInputStream(new BufferedInputStream(new FileInputStream(jarFile)));

        try {
            JarEntry jarEntry;

            while ((jarEntry = jin.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory() || !jarEntry.getName().toLowerCase().endsWith(".class"))
                    continue;

                processClass(trimClassExtension(jarEntry.getName().replace('/', '.')));
            }
        }
        finally {
            close(jin);
        }
    }

    /**
     * @param name Class name to process.
     */
    @SuppressWarnings( {"ErrorNotRethrown"})
    private void processClass(String name) {
        try {
            Class cls = Class.forName(name, false, OptimizedClassNamesGenerator.class.getClassLoader());

            if (isAccepted(cls)) {
                checkSerialVersionUid(cls);

                clsNames.add(name);
            }
        }
        catch (SecurityException | LinkageError ignored) {
            // No-op.
        }
        catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Checks serialVersionUID field in provided class.
     *
     * @param cls Class.
     */
    private void checkSerialVersionUid(Class cls) {
        // Check only Ignite classes.
        if (cls.isEnum() || cls.getSimpleName().isEmpty() || (!cls.getName().startsWith("org.gridgain.grid") &&
            !cls.getName().startsWith("org.gridgain.client") && !cls.getName().startsWith("org.apache.ignite")))
            return;

        try {
            Field field = cls.getDeclaredField("serialVersionUID");

            if (!field.getType().equals(long.class))
                throw new RuntimeException("serialVersionUID field is not long in class: " + cls.getName());

            int mod = field.getModifiers();

            if (!Modifier.isStatic(mod))
                throw new RuntimeException("serialVersionUID field is not static in class: " + cls.getName());

            if (!Modifier.isFinal(mod))
                throw new RuntimeException("serialVersionUID field is not final in class: " + cls.getName());
        }
        catch (NoSuchFieldException ignored) {
            clsWithoutSerialVersionUID.add(cls);
        }
    }

    /**
     * @param cls A class to examine.
     * @return Whether to add the class.
     */
    private static boolean isAccepted(Class cls) {
        return !(cls.isInterface() || (cls.getModifiers() & Modifier.ABSTRACT) != 0 || isTest(cls)) &&
            (cls.isEnum() || Serializable.class.isAssignableFrom(cls));
    }

    /**
     * Checks if class is a test case.
     *
     * @param cls Class.
     * @return {@code True} if test case.
     */
    private static boolean isTest(Class cls) {
        for (Class c = cls; c != Object.class; c = c.getSuperclass())
            if (c.getName().startsWith("junit.framework") || // JUnit3
                c.getName().startsWith("org.junit") || // JUnit4
                c.getName().startsWith("org.testing")) // TestNG
                return true;

        return false;
    }

    /**
     * @param path Path.
     * @return Trimmed path.
     */
    private static String trimClassExtension(String path) {
        return path.substring(0, path.length() - 6);
    }

    /**
     * @param closeable An object to close.
     */
    private static void close(Closeable closeable) {
        try {
            closeable.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args Command line arguments, none expected.
     * @throws Exception If generation failed.
     */
    public static void main(String[] args) throws Exception {
        File dir;

        if (args.length > 0 && args[0] != null && !args[0].isEmpty())
            dir = new File(args[0], "/org/apache/ignite/marshaller/optimized");
        else {
            String home = home();

            if (home == null)
                throw new Exception("Failed to find Ignite home.");

            dir = new File(home, "modules/core/src/main/java/org/apache/ignite/marshaller/optimized");
        }

        if (!dir.exists())
            throw new Exception("Optimized marshaller path does not exist: " + dir);

        if (!dir.isDirectory())
            throw new Exception("Destination path is not a directory: " + dir);

        new OptimizedClassNamesGenerator().writeClassNames(new File(dir, FILE_NAME), new File(dir, PREV_FILE_NAME));
    }

    /**
     * Retrieves {@code IGNITE_HOME} property. The property is retrieved from system
     * properties or from environment in that order.
     *
     * @return {@code IGNITE_HOME} property.
     */
    private static String home() {
        String home = System.getProperty(IGNITE_HOME_SYS_PROP);

        if (home == null || home.isEmpty()) {
            home = System.getenv(IGNITE_HOME_ENV_VAR);

            if (home == null || home.isEmpty())
                return null;
        }

        return home;
    }
}
