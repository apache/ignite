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
     * @return The Hadoop locations, never null.
     */
    public static HadoopLocations hadoopLocations() throws IOException {
        final String hadoopHome = systemOrEnv("HADOOP_HOME", systemOrEnv("HADOOP_PREFIX", null));

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
    private static Collection<SearchDirectory> classpathDirectories() throws IOException {
        HadoopLocations loc = hadoopLocations();

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
     * Resolves a Hadoop location directory.
     *
     * @param envVarName Environment variable name. The value denotes the location path.
     * @param hadoopHome Hadoop home location, may be null.
     * @param expHadoopHomeRelativePath The path relative to Hadoop home, expected to start with path separator.
     * @throws IOException If the value cannot be resolved to an existing directory.
     */
    private static String resolveLocation(String envVarName, String hadoopHome, String expHadoopHomeRelativePath)
        throws IOException {
        String val = systemOrEnv(envVarName, null);

        if (val == null) {
            // The env. variable is not set. Try to resolve the location relative HADOOP_HOME:
            if (!directoryExists(hadoopHome))
                throw new IOException("Failed to resolve Hadoop installation location. " +
                        envVarName + " or HADOOP_HOME environment variable should be set.");

            val = hadoopHome + expHadoopHomeRelativePath;
        }

        if (!directoryExists(val))
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
    private static boolean directoryExists(String path) {
        if (path == null)
            return false;

        Path p = Paths.get(path);

        return Files.exists(p) && Files.isDirectory(p) && Files.isReadable(p);
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

            if (!directoryExists(dir.getAbsolutePath()))
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
