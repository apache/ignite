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

package org.apache.ignite.internal.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Finds configuration files located in {@code IGNITE_HOME} folder
 * and its subfolders.
 */
public final class GridConfigurationFinder {
    /** Path to default configuration file. */
    private static final String DFLT_CFG = "config" + File.separator + "default-config.xml";

    /** Prefix for questionable paths. */
    public static final String Q_PREFIX = "(?)";

    /** */
    private static final int Q_PREFIX_LEN = Q_PREFIX.length();

    /**
     * Ensure singleton.
     */
    private GridConfigurationFinder() {
        // no-op
    }

    /**
     * Lists paths to all Ignite configuration files located in IGNITE_HOME with their
     * last modification timestamps.
     *
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    public static List<GridTuple3<String, Long, File>> getConfigFiles() throws IOException {
        return getConfigFiles(new File(U.getIgniteHome()));
    }

    /**
     * Lists paths to all Ignite configuration files located in given directory with their
     * last modification timestamps.
     *
     * @param dir Directory.
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    private static List<GridTuple3<String, Long, File>> getConfigFiles(File dir) throws IOException {
        assert dir != null;

        LinkedList<GridTuple3<String, Long, File>> lst = listFiles(dir);

        // Sort.
        Collections.sort(lst, new Comparator<GridTuple3<String, Long, File>>() {
            @Override public int compare(GridTuple3<String, Long, File> t1, GridTuple3<String, Long, File> t2) {
                String s1 = t1.get1();
                String s2 = t2.get1();


                String q1 = s1.startsWith(Q_PREFIX) ? s1.substring(Q_PREFIX_LEN + 1) : s1;
                String q2 = s2.startsWith(Q_PREFIX) ? s2.substring(Q_PREFIX_LEN + 1) : s2;

                return q1.compareTo(q2);
            }
        });

        File dflt = new File(U.getIgniteHome() + File.separator + DFLT_CFG);

        if (dflt.exists())
            lst.addFirst(F.t(DFLT_CFG, dflt.lastModified(), dflt));

        return lst;
    }

    /**
     * Lists paths to all Ignite configuration files located in given directory with their
     * last modification timestamps.
     *
     * NOTE: default configuration path will be skipped.
     *
     * @param dir Directory.
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    private static LinkedList<GridTuple3<String, Long, File>> listFiles(File dir) throws IOException {
        assert dir != null;

        LinkedList<GridTuple3<String, Long, File>> paths = new LinkedList<>();

        String[] configs = dir.list();

        if (configs != null)
            for (String name : configs) {
                File file = new File(dir, name);

                if (file.isDirectory())
                    paths.addAll(listFiles(file));
                else if (file.getName().endsWith(".xml")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        boolean springCfg = false;
                        boolean ggCfg = false;

                        String line;

                        while ((line = reader.readLine()) != null) {
                            if (line.contains("http://www.springframework.org/schema/beans"))
                                springCfg = true;

                            if (line.contains("class=\"org.apache.ignite.configuration.IgniteConfiguration\""))
                                ggCfg = true;

                            if (springCfg && ggCfg)
                                break;
                        }

                        if (springCfg) {
                            String path = file.getAbsolutePath().substring(U.getIgniteHome().length());

                            if (path.startsWith(File.separator))
                                path = path.substring(File.separator.length());

                            if (!path.equals(DFLT_CFG)) {
                                if (!ggCfg)
                                    path = Q_PREFIX + ' ' + path;

                                paths.add(F.t(path, file.lastModified(), file));
                            }
                        }
                    }
                }
            }

        return paths;
    }
}