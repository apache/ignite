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
    public static List<String> classpathForJavaProcess() throws IOException {
        List<String> res = new ArrayList<>();

        for (final SearchDirectory dir : classpathDirectories()) {
            if (dir.hasFilter()) {
                for (File file : dir.files())
                    res.add(file.getAbsolutePath());
            }
            else
                res.add(dir.dir.getAbsolutePath() + File.separator + '*');
        }

        return res;
    }

    /**
     * Gets Hadoop class path as a list of URLs (for in-process class loader usage).
     *
     * @return List of class path URLs.
     * @throws IOException If failed.
     */
    public static List<URL> classpathUrls() throws IOException {
        List<URL> res = new ArrayList<>();

        for (SearchDirectory dir : classpathDirectories()) {
            for (File file : dir.files()) {
                try {
                    res.add(file.toURI().toURL());
                }
                catch (MalformedURLException e) {
                    throw new IOException("Failed to convert file path to URL: " + file.getPath());
                }
            }
        }

        return res;
    }

    /**
     * @return HADOOP_HOME Variable.
     */
    private static String hadoopHome() {
        String prefix = systemOrEnv("HADOOP_PREFIX", null);

        return systemOrEnv("HADOOP_HOME", prefix);
    }

    /**
     * Gets locations from the environment.
     *
     * @return The locations as determined from the environment.
     */
    private static HadoopLocations getEnvHadoopLocations() {
        return new HadoopLocations(
            hadoopHome(),
            systemOrEnv("HADOOP_COMMON_HOME", null),
            systemOrEnv("HADOOP_HDFS_HOME", null),
            systemOrEnv("HADOOP_MAPRED_HOME", null)
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
     * Gets Hadoop locations.
     *
     * @return The locations as determined from the environment.
     */
    private static HadoopLocations getHadoopLocations() throws IOException {
        // 1. Get Hadoop locations from the environment:
        HadoopLocations loc = getEnvHadoopLocations();

        if (loc.isDefined())
            return loc.existsOrException();

        final String hadoopHome = hadoopHome();

        if (hadoopHome != null) {
            // If home is defined, it must exist:
            if (!directoryExists(hadoopHome))
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
    private static Collection<SearchDirectory> classpathDirectories() throws IOException {
        HadoopLocations loc = getHadoopLocations();

        Collection<SearchDirectory> res = new ArrayList<>();

        res.add(new SearchDirectory(new File(loc.commonHome(), "lib"), null));
        res.add(new SearchDirectory(new File(loc.hdfsHome(), "lib"), null));
        res.add(new SearchDirectory(new File(loc.mapredHome(), "lib"), null));

        res.add(new SearchDirectory(new File(loc.commonHome()), "hadoop-common-"));
        res.add(new SearchDirectory(new File(loc.commonHome()), "hadoop-auth-"));

        res.add(new SearchDirectory(new File(loc.hdfsHome()), "hadoop-hdfs-"));

        res.add(new SearchDirectory(new File(loc.mapredHome()), "hadoop-mapreduce-client-common"));
        res.add(new SearchDirectory(new File(loc.mapredHome()), "hadoop-mapreduce-client-core"));

        return res;
    }

    /**
     * Note that this method does not treat empty value as an absent value.
     *
     * @param name Variable name.
     * @param dflt Default.
     * @return Value.
     */
    private static String systemOrEnv(String name, String dflt) {
        String res = System.getProperty(name);

        if (res == null)
            res = System.getenv(name);

        return res == null ? dflt : res;
    }

    /**
     * Answers if the given path denotes existing directory.
     *
     * @param path The directory path.
     * @return {@code True} if the given path denotes an existing directory.
     */
    static boolean directoryExists(String path) {
        if (path == null)
            return false;

        Path p = Paths.get(path);

        return Files.exists(p) && Files.isDirectory(p) && Files.isReadable(p);
    }

    /**
     * Simple pair-like structure to hold directory name and a mask assigned to it.
     */
    public static class SearchDirectory {
        /** File. */
        private final File dir;

        /** The mask. */
        private final String filter;

        /**
         * Constructor.
         *
         * @param dir Directory.
         * @param filter Filter.
         */
        private SearchDirectory(File dir, String filter) throws IOException {
            this.dir = dir;
            this.filter = filter;

            if (!directoryExists(dir.getAbsolutePath()))
                throw new IOException("Directory cannot be read: " + dir.getAbsolutePath());
        }

        /**
         * @return Child files.
         */
        private File[] files() throws IOException {
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return filter == null || name.startsWith(filter);
                }
            });

            if (files == null)
                throw new IOException("Path is not a directory. [dir=" + dir + ']');

            return files;
        }

        /**
         * @return {@code True} if filter exists.
         */
        private boolean hasFilter() {
            return filter != null;
        }
    }
}