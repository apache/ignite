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

package org.apache.ignite.examples.ml.clustering;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.ml.clustering.KMeansLocalClusterer;
import org.apache.ignite.ml.clustering.KMeansModel;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link KMeansLocalClusterer}.
 */
public class KMeansLocalClustererExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        // IMPL NOTE based on KMeansDistributedClustererTestSingleNode#testClusterizationOnDatasetWithObviousStructure
        System.out.println(">>> K-means local clusterer example started.");

        int ptsCnt = 10000;
        DenseLocalOnHeapMatrix points = new DenseLocalOnHeapMatrix(ptsCnt, 2);

        DatasetWithObviousStructure dataset = new DatasetWithObviousStructure(10000);

        List<Vector> massCenters = dataset.generate(points);

        EuclideanDistance dist = new EuclideanDistance();
        OrderedNodesComparator comp = new OrderedNodesComparator(
            dataset.centers().values().toArray(new Vector[] {}), dist);

        massCenters.sort(comp);

        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(dist, 100, 1L);

        KMeansModel mdl = clusterer.cluster(points, 4);
        Vector[] resCenters = mdl.centers();
        Arrays.sort(resCenters, comp);

        System.out.println("Mass centers:");
        massCenters.forEach(Tracer::showAscii);

        System.out.println("Cluster centers:");
        Arrays.asList(resCenters).forEach(Tracer::showAscii);

        System.out.println("\n>>> K-means local clusterer example completed.");
    }

    /** */
    private static class OrderedNodesComparator implements Comparator<Vector> {
        /** */
        private final DistanceMeasure measure;

        /** */
        List<Vector> orderedNodes;

        /** */
        OrderedNodesComparator(Vector[] orderedNodes, DistanceMeasure measure) {
            this.orderedNodes = Arrays.asList(orderedNodes);
            this.measure = measure;
        }

        /** */
        private int findClosestNodeIndex(Vector v) {
            return Functions.argmin(orderedNodes, v1 -> measure.compute(v1, v)).get1();
        }

        /** */
        @Override public int compare(Vector v1, Vector v2) {
            int ind1 = findClosestNodeIndex(v1);
            int ind2 = findClosestNodeIndex(v2);

            int signum = (int)Math.signum(ind1 - ind2);

            if (signum != 0)
                return signum;

            return (int)Math.signum(orderedNodes.get(ind1).minus(v1).kNorm(2) -
                orderedNodes.get(ind2).minus(v2).kNorm(2));
        }
    }
}
