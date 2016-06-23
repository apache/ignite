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

package org.apache.ignite.internal.processors.hadoop;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Hadoop classpath utilities.
 */
public class HadoopClasspathUtils {
    /**
     * Gets Hadoop class path as list of classpath elements for process.
     *
     * @return List of the class path elements.
     * @throws IOException If failed.
     */
    static List<String> getAsProcessClasspath() throws IOException {
        Collection<DirAndMask> dams = getClasspathBaseDirectories();

        List<String> list = new ArrayList<>(32);

        for (DirAndMask dam: dams)
            addAsJavaProcessClasspathElement(list, dam.dir, dam.mask);

        return list;
    }

    /**
     * Gets Hadoop class path as a list of URLs (for in-process class loader usage).
     *
     * @return List of class path URLs.
     * @throws IOException If failed.
     */
    public static List<URL> getAsUrlList() throws IOException {
        Collection<DirAndMask> dams = getClasspathBaseDirectories();

        List<URL> list = new ArrayList<>(32);

        for (DirAndMask dam: dams)
            // Note that this procedure does not use '*' classpath patterns,
            // but adds all the children explicitly:
            addUrls(list, dam.dir, dam.mask);

        return list;
    }

    /**
     * Discovers classpath entries in specified directory and adds them as URLs to the given {@code res} collection.
     *
     * @param res Result.
     * @param dir Directory.
     * @param startsWith Starts with prefix.
     * @throws IOException If failed.
     */
    private static void addUrls(Collection<URL> res, File dir, final String startsWith) throws IOException {
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return startsWith == null || name.startsWith(startsWith);
            }
        });

        if (files == null)
            throw new IOException("Path is not a directory. [dir=" + dir + ']');

        for (File file : files) {
            try {
                res.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new IOException("Failed to convert file path to URL: " + file.getPath());
            }
        }
    }

    /**
     * Discovers classpath entries in specified directory and adds them as URLs to the given {@code res} collection.
     *
     * @param res Result.
     * @param dir Directory.
     * @param startsWith Starts with prefix.
     * @throws IOException If failed.
     */
    private static void addAsJavaProcessClasspathElement(Collection<String> res, File dir, final String startsWith)
        throws IOException {
        if (!dir.exists() || !dir.isDirectory() || !dir.canRead())
            throw new IOException("Path is not an existing readable directory. [dir=" + dir + ']');

        if (startsWith == null)
            res.add(dir.getAbsolutePath() + File.separator + '*');
        else {
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith(startsWith);
                }
            });

            if (files == null)
                throw new IOException("Path is not a directory. [" + dir + ']');

            for (File file : files)
                res.add(file.getAbsolutePath());
        }
    }

    /**
     * @return HADOOP_HOME Variable.
     */
    private static String hadoopHome() {
        String prefix = getEnv("HADOOP_PREFIX", null);

        return getEnv("HADOOP_HOME", prefix);
    }

    /**
     * Simple structure to hold Hadoop directory locations.
     */
    public static class HadoopLocations {
        /** HADOOP_HOME, may be null. */
        public final String home;
        /** HADOOP_COMMON_HOME */
        public final String common;
        /** HADOOP_HDFS_HOME */
        public final String hdfs;
        /** HADOOP_MAPRED_HOME */
        public final String mapred;

        /**
         * Constructor.
         *
         * @param home HADOOP_HOME
         * @param common HADOOP_COMMON_HOME
         * @param hdfs HADOOP_HDFS_HOME
         * @param mapred HADOOP_MAPRED_HOME
         */
        HadoopLocations(String home, String common, String hdfs, String mapred) {
            this.home = home;
            this.common = common;
            this.hdfs = hdfs;
            this.mapred = mapred;
        }
    }

    /**
     * Gets Hadoop locations.
     *
     * @return The Hadoop locations, never null.
     */
    public static HadoopLocations getHadoopLocations() throws IOException {
        final String hadoopHome = hadoopHome();

        String commonHome = resolveLocation("HADOOP_COMMON_HOME", hadoopHome, "/share/hadoop/common");
        String hdfsHome = resolveLocation("HADOOP_HDFS_HOME", hadoopHome, "/share/hadoop/hdfs");
        String mapredHome = resolveLocation("HADOOP_MAPRED_HOME", hadoopHome, "/share/hadoop/mapreduce");

        return new HadoopLocations(hadoopHome, commonHome, hdfsHome, mapredHome);
    }

    /**
     * Gets base directories to discover classpath elements in.
     *
     * @return Collection of directory and mask pairs.
     * @throws IOException if a mandatory classpath location is not found.
     */
    private static Collection<DirAndMask> getClasspathBaseDirectories() throws IOException {
        HadoopLocations loc = getHadoopLocations();

        Collection<DirAndMask> c = new ArrayList<>();

        c.add(new DirAndMask(new File(loc.common, "lib"), null));
        c.add(new DirAndMask(new File(loc.hdfs, "lib"), null));
        c.add(new DirAndMask(new File(loc.mapred, "lib"), null));

        c.add(new DirAndMask(new File(loc.common), "hadoop-common-"));
        c.add(new DirAndMask(new File(loc.common), "hadoop-auth-"));

        c.add(new DirAndMask(new File(loc.hdfs), "hadoop-hdfs-"));

        c.add(new DirAndMask(new File(loc.mapred), "hadoop-mapreduce-client-common"));
        c.add(new DirAndMask(new File(loc.mapred), "hadoop-mapreduce-client-core"));

        return c;
    }

    /**
     * Simple pair-like structure to hold directory name and a mask assigned to it.
     */
    public static class DirAndMask {
        /**
         * Constructor.
         *
         * @param dir The directory.
         * @param mask The mask.
         */
        DirAndMask(File dir, String mask) {
            this.dir = dir;
            this.mask = mask;
        }

        /** The path. */
        public final File dir;

        /** The mask. */
        public final String mask;
    }

    /**
     * Resolves a Hadoop location directory.
     *
     * @param envVarName Environment variable name. The value denotes the location path.
     * @param hadoopHome Hadoop home location, may be null.
     * @param expHadoopHomeRelativePath The path relative to Hadoop home, expected to start with path separator.
     * @throws IOException If the value cannot be resolved to an existing directory.
     */
    private static String resolveLocation(String envVarName, String hadoopHome, String expHadoopHomeRelativePath)
        throws IOException {
        String val = getEnv(envVarName, null);

        if (val == null) {
            // The env. variable is not set. Try to resolve the location relative HADOOP_HOME:
            if (!isExistingDirectory(hadoopHome))
                throw new IOException("Failed to resolve Hadoop installation location. " +
                        envVarName + " or HADOOP_HOME environment variable should be set.");

            val = hadoopHome + expHadoopHomeRelativePath;
        }

        if (!isExistingDirectory(val))
            throw new IOException("Failed to resolve Hadoop location [path=" + val + ']');

        return val;
    }

    /**
     * Note that this method does not treat empty value as an absent value.
     *
     * @param name Variable name.
     * @param dflt Default.
     * @return Value.
     */
    private static String getEnv(String name, String dflt) {
        String res = System.getProperty(name);

        if (res == null)
            res = System.getenv(name);

        return res == null ? dflt : res;
    }

    /**
     * Answers if the given path denotes existing directory.
     *
     * @param path The directory path.
     * @return 'true' if the given path denotes an existing directory.
     */
    private static boolean isExistingDirectory(String path) {
        if (path == null)
            return false;

        Path p = Paths.get(path);

        return Files.exists(p) && Files.isDirectory(p) && Files.isReadable(p);
    }
}
