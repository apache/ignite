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

package org.apache.ignite.examples;

import java.net.URL;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;

/**
 *
 */
public class ExamplesUtils {
    /** */
    private static final ClassLoader CLS_LDR = ExamplesUtils.class.getClassLoader();

    /**
     * Exits with code {@code -1} if maximum memory is below 90% of minimally allowed threshold.
     *
     * @param min Minimum memory threshold.
     */
    public static void checkMinMemory(long min) {
        long maxMem = Runtime.getRuntime().maxMemory();

        if (maxMem < .85 * min) {
            System.err.println("Heap limit is too low (" + (maxMem / (1024 * 1024)) +
                "MB), please increase heap size at least up to " + (min / (1024 * 1024)) + "MB.");

            System.exit(-1);
        }
    }

    /**
     * Returns URL resolved by class loader for classes in examples project.
     *
     * @return Resolved URL.
     */
    public static URL url(String path) {
        URL url = CLS_LDR.getResource(path);

        if (url == null)
            throw new RuntimeException("Failed to resolve resource URL by path: " + path);

        return url;
    }

    /**
     * Checks minimum topology size for running a certain example.
     *
     * @param grp Cluster to check size for.
     * @param size Minimum number of nodes required to run a certain example.
     * @return {@code True} if check passed, {@code false} otherwise.
     */
    public static boolean checkMinTopologySize(ClusterGroup grp, int size) {
        int prjSize = grp.nodes().size();

        if (prjSize < size) {
            System.err.println(">>> Please start at least " + size + " cluster nodes to run example.");

            return false;
        }

        return true;
    }

    /**
     * Checks if cluster has server nodes.
     *
     * @param ignite Ignite instance.
     * @return {@code True} if cluster has server nodes, {@code false} otherwise.
     */
    public static boolean hasServerNodes(Ignite ignite) {
        if (ignite.cluster().forServers().nodes().isEmpty()) {
            System.err.println("Server nodes not found (start data nodes with ExampleNodeStartup class)");

            return false;
        }

        return true;
    }

    /**
     * Convenience method for printing query results.
     *
     * @param res Query results.
     */
    public static void printQueryResults(List<?> res) {
        if (res == null || res.isEmpty())
            System.out.println("Query result set is empty.");
        else {
            for (Object row : res) {
                if (row instanceof List) {
                    System.out.print("(");

                    List<?> l = (List)row;

                    for (int i = 0; i < l.size(); i++) {
                        Object o = l.get(i);

                        if (o instanceof Double || o instanceof Float)
                            System.out.printf("%.2f", o);
                        else
                            System.out.print(l.get(i));

                        if (i + 1 != l.size())
                            System.out.print(',');
                    }

                    System.out.println(')');
                }
                else
                    System.out.println("  " + row);
            }
        }
    }
}