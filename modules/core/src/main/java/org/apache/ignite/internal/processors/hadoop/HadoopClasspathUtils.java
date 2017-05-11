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
import java.util.LinkedList;
import java.util.List;

/**
 * Hadoop classpath utilities.
 */
public class HadoopClasspathUtils {
    /** Prefix directory. */
    public static final String PREFIX = "HADOOP_PREFIX";

    /** Hadoop home directory. */
    public static final String HOME = "HADOOP_HOME";

    /** Hadoop common directory. */
    public static final String COMMON_HOME = "HADOOP_COMMON_HOME";

    /** Hadoop HDFS directory. */
    public static final String HDFS_HOME = "HADOOP_HDFS_HOME";

    /** Hadoop mapred directory. */
    public static final String MAPRED_HOME = "HADOOP_MAPRED_HOME";

    /** Arbitrary additional dependencies. Compliant with standard Java classpath resolution. */
    public static final String HADOOP_USER_LIBS = "HADOOP_USER_LIBS";

    /** Empty string. */
    private static final String EMPTY_STR = "";

    /**
     * Gets Hadoop class path as a list of URLs (for in-process class loader usage).
     *
     * @return List of class path URLs.
     * @throws IOException If failed.
     */
    public static List<URL> classpathForClassLoader() throws IOException {
        List<URL> res = new ArrayList<>();

        for (SearchDirectory dir : classpathDirectories()) {
            for (File file : dir.files()) {
                try {
                    res.add(file.toURI().toURL());
                }
                catch (MalformedURLException ignored) {
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

        // Add libraries from Hadoop distribution:
        res.add(new SearchDirectory(new File(loc.common(), "lib"), AcceptAllDirectoryFilter.INSTANCE));
        res.add(new SearchDirectory(new File(loc.hdfs(), "lib"), AcceptAllDirectoryFilter.INSTANCE));
        res.add(new SearchDirectory(new File(loc.mapred(), "lib"), AcceptAllDirectoryFilter.INSTANCE));

        res.add(new SearchDirectory(new File(loc.common()), new PrefixDirectoryFilter("hadoop-common-")));
        res.add(new SearchDirectory(new File(loc.common()), new PrefixDirectoryFilter("hadoop-auth-")));

        res.add(new SearchDirectory(new File(loc.hdfs()), new PrefixDirectoryFilter("hadoop-hdfs-")));

        res.add(new SearchDirectory(new File(loc.mapred()),
            new PrefixDirectoryFilter("hadoop-mapreduce-client-common")));
        res.add(new SearchDirectory(new File(loc.mapred()),
            new PrefixDirectoryFilter("hadoop-mapreduce-client-core")));

        // Add user provided libs:
        res.addAll(parseUserLibs());

        return res;
    }

    /**
     * Parse user libs.
     *
     * @return Parsed libs search patterns.
     * @throws IOException If failed.
     */
    public static Collection<SearchDirectory> parseUserLibs() throws IOException {
        return parseUserLibs(systemOrEnv(HADOOP_USER_LIBS, null));
    }

    /**
     * Parse user libs.
     *
     * @param str String.
     * @return Result.
     * @throws IOException If failed.
     */
    public static Collection<SearchDirectory> parseUserLibs(String str) throws IOException {
        Collection<SearchDirectory> res = new LinkedList<>();

        if (!isEmpty(str)) {
            String[] tokens = normalize(str).split(File.pathSeparator);

            for (String token : tokens) {
                // Skip empty tokens.
                if (isEmpty(token))
                    continue;

                File file = new File(token);
                File dir = file.getParentFile();

                if (token.endsWith("*")) {
                    assert dir != null;

                    res.add(new SearchDirectory(dir, AcceptAllDirectoryFilter.INSTANCE, false));
                }
                else {
                    // Met "/" or "C:\" pattern, nothing to do with it.
                    if (dir == null)
                        continue;

                    res.add(new SearchDirectory(dir, new ExactDirectoryFilter(file.getName()), false));
                }
            }
        }

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
     * NOramlize the string.
     *
     * @param str String.
     * @return Normalized string.
     */
    private static String normalize(String str) {
        assert str != null;

        return str.trim().toLowerCase();
    }

    /**
     * Simple pair-like structure to hold directory name and a mask assigned to it.
     */
    public static class SearchDirectory {
        /** File. */
        private final File dir;

        /** Filter. */
        private final DirectoryFilter filter;

        /** Whether directory must exist. */
        private final boolean strict;

        /**
         * Constructor for directory search with strict rule.
         *
         * @param dir Directory.
         * @param filter Filter.
         * @throws IOException If failed.
         */
        private SearchDirectory(File dir, DirectoryFilter filter) throws IOException {
            this(dir, filter, true);
        }

        /**
         * Constructor.
         *
         * @param dir Directory.
         * @param filter Filter.
         * @param strict Whether directory must exist.
         * @throws IOException If failed.
         */
        private SearchDirectory(File dir, DirectoryFilter filter, boolean strict) throws IOException {
            this.dir = dir;
            this.filter = filter;
            this.strict = strict;

            if (strict && !exists(dir.getAbsolutePath()))
                throw new IOException("Directory cannot be read: " + dir.getAbsolutePath());
        }

        /**
         * @return Child files.
         */
        public File[] files() throws IOException {
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return filter.test(name);
                }
            });

            if (files == null) {
                if (strict)
                    throw new IOException("Failed to get directory files [dir=" + dir + ']');
                else
                    return new File[0];
            }
            else
                return files;
        }
    }

    /**
     * Directory filter interface.
     */
    public static interface DirectoryFilter {
        /**
         * Test if file with this name should be included.
         *
         * @param name File name.
         * @return {@code True} if passed.
         */
        public boolean test(String name);
    }

    /**
     * Filter to accept all files.
     */
    public static class AcceptAllDirectoryFilter implements DirectoryFilter {
        /** Singleton instance. */
        public static final AcceptAllDirectoryFilter INSTANCE = new AcceptAllDirectoryFilter();

        /** {@inheritDoc} */
        @Override public boolean test(String name) {
            return true;
        }
    }

    /**
     * Filter which uses prefix to filter files.
     */
    public static class PrefixDirectoryFilter implements DirectoryFilter {
        /** Prefix. */
        private final String prefix;

        /**
         * Constructor.
         *
         * @param prefix Prefix.
         */
        public PrefixDirectoryFilter(String prefix) {
            assert prefix != null;

            this.prefix = normalize(prefix);
        }

        /** {@inheritDoc} */
        @Override public boolean test(String name) {
            return normalize(name).startsWith(prefix);
        }
    }

    /**
     * Filter which uses exact comparison.
     */
    public static class ExactDirectoryFilter implements DirectoryFilter {
        /** Name. */
        private final String name;

        /**
         * Constructor.
         *
         * @param name Name.
         */
        public ExactDirectoryFilter(String name) {
            this.name = normalize(name);
        }

        /** {@inheritDoc} */
        @Override public boolean test(String name) {
            return normalize(name).equals(this.name);
        }
    }
}
