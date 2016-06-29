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
    /** Prefix directory. */
    public static final String PREFIX = "HADOOP_PREFIX";

    /** Home directory. */
    public static final String HOME = "HADOOP_HOME";

    /** Home directory. */
    public static final String COMMON_HOME = "HADOOP_COMMON_HOME";

    /** Home directory. */
    public static final String HDFS_HOME = "HADOOP_HDFS_HOME";

    /** Home directory. */
    public static final String MAPRED_HOME = "HADOOP_MAPRED_HOME";

    /** Empty string. */
    private static final String EMPTY_STR = "";

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
                res.add(dir.absolutePath() + File.separator + '*');
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
     * Gets Hadoop locations.
     *
     * @return The locations as determined from the environment.
     */
    public static HadoopLocations locations() throws IOException {
        // Query environment.
        String hadoopHome = systemOrEnv(PREFIX, systemOrEnv(HOME, EMPTY_STR));

        String commonHome = systemOrEnv(COMMON_HOME, EMPTY_STR);
        String hdfsHome = systemOrEnv(HDFS_HOME, EMPTY_STR);
        String mapredHome = systemOrEnv(MAPRED_HOME, EMPTY_STR);

        // If any composite location is defined, use only them.
        if (!isEmpty(commonHome) || !isEmpty(hdfsHome) || !isEmpty(mapredHome)) {
            HadoopLocations res = new HadoopLocations(hadoopHome, commonHome, hdfsHome, mapredHome);

            if (res.valid())
                return res;
            else
                throw new IOException("Failed to resolve Hadoop classpath because some environment variables are " +
                    "either undefined or point to nonexistent directories [" +
                    "[env=" + COMMON_HOME + ", value=" + commonHome + ", exists=" + res.commonExists() + "], " +
                    "[env=" + HDFS_HOME + ", value=" + hdfsHome + ", exists=" + res.hdfsExists() + "], " +
                    "[env=" + MAPRED_HOME + ", value=" + mapredHome + ", exists=" + res.mapredExists() + "]]");
        }
        else if (!isEmpty(hadoopHome)) {
            // All further checks will be based on HADOOP_HOME, so check for it's existence.
            if (!exists(hadoopHome))
                throw new IOException("Failed to resolve Hadoop classpath because " + HOME + " environment " +
                    "variable points to nonexistent directory: " + hadoopHome);

            // Probe Apache Hadoop.
            HadoopLocations res = new HadoopLocations(
                hadoopHome,
                hadoopHome + "/share/hadoop/common",
                hadoopHome + "/share/hadoop/hdfs",
                hadoopHome + "/share/hadoop/mapreduce"
            );

            if (res.valid())
                return res;

            // Probe CDH.
            res = new HadoopLocations(
                hadoopHome,
                hadoopHome,
                hadoopHome + "/../hadoop-hdfs",
                hadoopHome + "/../hadoop-mapreduce"
            );

            if (res.valid())
                return res;

            // Probe HDP.
            res = new HadoopLocations(
                hadoopHome,
                hadoopHome,
                hadoopHome + "/../hadoop-hdfs-client",
                hadoopHome + "/../hadoop-mapreduce-client"
            );

            if (res.valid())
                return res;

            // Failed.
            throw new IOException("Failed to resolve Hadoop classpath because " + HOME + " environment variable " +
                "is either invalid or points to non-standard Hadoop distribution: " + hadoopHome);
        }
        else {
            // Advise to set HADOOP_HOME only as this is preferred way to configure classpath.
            throw new IOException("Failed to resolve Hadoop classpath (please define " + HOME + " environment " +
                "variable and point it to your Hadoop distribution).");
        }
    }

    /**
     * Gets base directories to discover classpath elements in.
     *
     * @return Collection of directory and mask pairs.
     * @throws IOException if a mandatory classpath location is not found.
     */
    private static Collection<SearchDirectory> classpathDirectories() throws IOException {
        HadoopLocations loc = locations();

        Collection<SearchDirectory> res = new ArrayList<>();

        res.add(new SearchDirectory(new File(loc.common(), "lib"), null));
        res.add(new SearchDirectory(new File(loc.hdfs(), "lib"), null));
        res.add(new SearchDirectory(new File(loc.mapred(), "lib"), null));

        res.add(new SearchDirectory(new File(loc.common()), "hadoop-common-"));
        res.add(new SearchDirectory(new File(loc.common()), "hadoop-auth-"));

        res.add(new SearchDirectory(new File(loc.hdfs()), "hadoop-hdfs-"));

        res.add(new SearchDirectory(new File(loc.mapred()), "hadoop-mapreduce-client-common"));
        res.add(new SearchDirectory(new File(loc.mapred()), "hadoop-mapreduce-client-core"));

        return res;
    }

    /**
     * Get system property or environment variable with the given name.
     *
     * @param name Variable name.
     * @param dflt Default value.
     * @return Value.
     */
    private static String systemOrEnv(String name, String dflt) {
        String res = System.getProperty(name);

        if (res == null)
            res = System.getenv(name);

        return res != null ? res : dflt;
    }

    /**
     * Answers if the given path denotes existing directory.
     *
     * @param path The directory path.
     * @return {@code True} if the given path denotes an existing directory.
     */
    public static boolean exists(String path) {
        if (path == null)
            return false;

        Path p = Paths.get(path);

        return Files.exists(p) && Files.isDirectory(p) && Files.isReadable(p);
    }

    /**
     * Check if string is empty.
     *
     * @param val Value.
     * @return {@code True} if empty.
     */
    private static boolean isEmpty(String val) {
        return val == null || val.isEmpty();
    }

    /**
     * Simple pair-like structure to hold directory name and a mask assigned to it.
     */
    private static class SearchDirectory {
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

            if (!exists(dir.getAbsolutePath()))
                throw new IOException("Directory cannot be read: " + dir.getAbsolutePath());
        }

        /**
         * @return Absolute path.
         */
        private String absolutePath() {
            return dir.getAbsolutePath();
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