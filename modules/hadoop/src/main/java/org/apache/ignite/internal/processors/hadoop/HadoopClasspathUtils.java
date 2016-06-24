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

        /**
         * Answers if all the base directories are defined.
         *
         * @return 'true' if "common", "hdfs", and "mapred" directories are defined.
         */
        public boolean isDefined() {
            return common != null && hdfs != null && mapred != null;
        }

        /**
         * Answers if all the base directories exist.
         *
         * @return 'true' if "common", "hdfs", and "mapred" directories do exist.
         */
        public boolean exists() {
            return isExistingDirectory(common)
                && isExistingDirectory(hdfs)
                && isExistingDirectory(mapred);
        }

        /**
         * Checks if all the base directories exist.
         *
         * @return this reference.
         * @throws IOException if any of the base directories does not exist.
         */
        public HadoopLocations existsOrException() throws IOException {
            if (!isExistingDirectory(common))
                throw new IOException("Failed to resolve Hadoop installation location. HADOOP_COMMON_HOME " +
                    "or HADOOP_HOME environment variable should be set.");

            if (!isExistingDirectory(hdfs))
                throw new IOException("Failed to resolve Hadoop installation location. HADOOP_HDFS_HOME " +
                    "or HADOOP_HOME environment variable should be set.");

            if (!isExistingDirectory(mapred))
                throw new IOException("Failed to resolve Hadoop installation location. HADOOP_MAPRED_HOME " +
                    "or HADOOP_HOME environment variable should be set.");

            return this;
        }
    }

    /**
     * Gets locations from the environment.
     *
     * @return The locations as determined from the environment.
     */
    private static HadoopLocations getEnvHadoopLocations() {
        return new HadoopLocations(
            hadoopHome(),
            getEnv("HADOOP_COMMON_HOME", null),
            getEnv("HADOOP_HDFS_HOME", null),
            getEnv("HADOOP_MAPRED_HOME", null)
        );
    }

    /**
     * Gets locations assuming Apache Hadoop distribution layout.
     *
     * @return The locations as for Apache distribution.
     */
    private static HadoopLocations getApacheHadoopLocations(String hadoopHome) {
        return new HadoopLocations(hadoopHome,
            hadoopHome + "/share/hadoop/common",
            hadoopHome + "/share/hadoop/hdfs",
            hadoopHome + "/share/hadoop/mapreduce");
    }

    /** HDP Hadoop locations. */
    private static final HadoopLocations HDP_HADOOP_LOCATIONS = new HadoopLocations(
        "/usr/hdp/current/hadoop-client",
        "/usr/hdp/current/hadoop-client",
        "/usr/hdp/current/hadoop-hdfs-client/",
        "/usr/hdp/current/hadoop-mapreduce-client/");

    /**
     * HDP locations relative to an arbitrary Hadoop home.
     *
     * @param hadoopHome The hadoop home.
     * @return The locations.
     */
    private static HadoopLocations getHdpLocationsRelative(String hadoopHome) {
        return new HadoopLocations(hadoopHome, hadoopHome,
            hadoopHome + "/../hadoop-hdfs-client/",
            hadoopHome + "/../hadoop-mapreduce-client/");
    }

    /**
     * Gets the existing Hadoop locations, if any.
     *
     * @return Existing Hadoop locations.
     * @throws IOException If no existing location found.
     */
    public static HadoopLocations getHadoopLocations() throws IOException {
        // 1. Try locations defined in System properties or environment:
        HadoopLocations loc = getEnvHadoopLocations();

        if (loc.isDefined())
            return loc.existsOrException();

        final String hadoopHome = hadoopHome();

        if (hadoopHome != null) {
            // If home is defined, it must exist:
            if (!isExistingDirectory(hadoopHome))
                throw new IOException("HADOOP_HOME location is not an existing readable directory. [dir="
                    + hadoopHome + ']');

            // 2. Try Apache Hadoop locations defined relative to HADOOP_HOME:
            loc = getApacheHadoopLocations(hadoopHome);

            if (loc.exists())
                return loc;

            // 3. Try HDP Hadoop locations defined relative to HADOOP_HOME:
            loc = getHdpLocationsRelative(hadoopHome);

            return loc.existsOrException();
        }

        // 4. Try absolute HDP (Hortonworks) location:
        return HDP_HADOOP_LOCATIONS.existsOrException();
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
