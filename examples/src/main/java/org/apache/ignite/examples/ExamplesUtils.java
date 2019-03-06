/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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