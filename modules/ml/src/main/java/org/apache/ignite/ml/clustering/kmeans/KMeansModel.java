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

package org.apache.ignite.ml.clustering.kmeans;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.inference.JSONModel;
import org.apache.ignite.ml.inference.JSONReadable;
import org.apache.ignite.ml.inference.JSONWritable;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.distances.ChebyshevDistance;
import org.apache.ignite.ml.math.distances.CosineSimilarity;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.distances.HammingDistance;
import org.apache.ignite.ml.math.distances.JaccardIndex;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.distances.MinkowskiDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * This class encapsulates result of clusterization by KMeans algorithm.
 */
public final class KMeansModel implements ClusterizationModel<Vector, Integer>, Exportable<KMeansModelFormat>,
    JSONWritable, JSONReadable, DeployableObject {
    /** Centers of clusters. */
    private Vector[] centers;

    /** Distance measure. */
    private DistanceMeasure distanceMeasure = new EuclideanDistance();

    /**
     * Construct KMeans model with given centers and distanceMeasure measure.
     *
     * @param centers Centers.
     * @param distanceMeasure Distance measure.
     */
    public KMeansModel(Vector[] centers, DistanceMeasure distanceMeasure) {
        this.centers = centers;
        this.distanceMeasure = distanceMeasure;
    }

    /** {@inheritDoc} */
    public KMeansModel() {

    }

    /** Distance measure. */
    public DistanceMeasure distanceMeasure() {
        return distanceMeasure;
    }

    /** {@inheritDoc} */
    @Override public int amountOfClusters() {
        return centers.length;
    }

    /**
     * Set up the centroids.
     *
     * @param centers The parameter value.
     * @return Model with new centers parameter value.
     */
    public KMeansModel withCentroids(Vector[] centers) {
        this.centers = centers;
        return this;
    }

    /**
     * Set up the distance measure.
     *
     * @param distanceMeasure The parameter value.
     * @return Model with new distance measure parameter value.
     */
    public KMeansModel withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector[] centers() {
        return Arrays.copyOf(centers, centers.length);
    }

    /** {@inheritDoc} */
    @Override public Integer predict(Vector vec) {
        int res = -1;
        double minDist = Double.POSITIVE_INFINITY;

        for (int i = 0; i < centers.length; i++) {
            double curDist = distanceMeasure.compute(centers[i], vec);
            if (curDist < minDist) {
                minDist = curDist;
                res = i;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<KMeansModelFormat, P> exporter, P path) {
        KMeansModelFormat mdlData = new KMeansModelFormat(centers, distanceMeasure);

        exporter.save(mdlData, path);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + Arrays.hashCode(centers);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KMeansModel that = (KMeansModel)obj;

        return distanceMeasure.equals(that.distanceMeasure) && Arrays.deepEquals(centers, that.centers);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        String measureName = distanceMeasure.getClass().getSimpleName();
        List<String> centersList = Arrays.stream(centers).map(x -> Tracer.asAscii(x, "%.4f", false))
            .collect(Collectors.toList());

        return ModelTrace.builder("KMeansModel", pretty)
            .addField("distance measure", measureName)
            .addField("centroids", centersList)
            .toString();
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(distanceMeasure);
    }

    /** {@inheritDoc} */
    @Override public KMeansModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

            KMeansJSONExportModel exportModel;
            try {
                exportModel = mapper
                        .readValue(new File(path.toAbsolutePath().toString()), KMeansJSONExportModel.class);

                return exportModel.convert();
            } catch (IOException e) {
                e.printStackTrace();
            }
        return null;
    }

    // TODO: https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/pmml/export/KMeansPMMLModelExport.scala
    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
            ObjectMapper mapper = new ObjectMapper();

            try {
                KMeansJSONExportModel exportModel = new KMeansJSONExportModel();
                List<double[]> listOfCenters = new ArrayList<>();
                for (int i = 0; i < centers.length; i++) {
                    listOfCenters.add(centers[i].asArray());
                }

                exportModel.mdlCenters = listOfCenters;
                exportModel.versionName = "2.9.0-SNAPSHOT";
                exportModel.distanceMeasureName = distanceMeasure.getClass().getSimpleName();

                if(distanceMeasure instanceof MinkowskiDistance) {
                    exportModel.pNorm = ((MinkowskiDistance) distanceMeasure).p();
                }

                File file = new File(path.toAbsolutePath().toString());
                mapper.writeValue(file, exportModel);
            } catch (IOException e) {
                e.printStackTrace();
            }

    }


    private static class KMeansJSONExportModel extends JSONModel {
        /** Centers of clusters. */
        public List<double[]> mdlCenters;

        /** Distance measure. */
        public String distanceMeasureName;

        /** p - norm for Minkowski distance only. */
        public double pNorm = 0;

        public KMeansModel convert() {
            KMeansModel mdl = new KMeansModel();
            Vector[] centers = new DenseVector[mdlCenters.size()];
            for (int i = 0; i < mdlCenters.size(); i++) {
                centers[i] = VectorUtils.of(mdlCenters.get(i));
            }

            DistanceMeasure distanceMeasure;

            // TODO: add new distances
            if(distanceMeasureName.equals(EuclideanDistance.class.getSimpleName())) {
                distanceMeasure = new EuclideanDistance();
            } else if (distanceMeasureName.equals(MinkowskiDistance.class.getSimpleName())) {
                distanceMeasure = new MinkowskiDistance(pNorm); // TODO: add test for this
            } else if (distanceMeasureName.equals(ChebyshevDistance.class.getSimpleName())) {
                distanceMeasure = new ChebyshevDistance();
            } else if (distanceMeasureName.equals(ManhattanDistance.class.getSimpleName())) {
                distanceMeasure = new ManhattanDistance();
            } else if (distanceMeasureName.equals(HammingDistance.class.getSimpleName())) {
                distanceMeasure = new HammingDistance();
            } else if(distanceMeasureName.equals(JaccardIndex.class.getSimpleName())) {
                distanceMeasure = new JaccardIndex();
            } else if(distanceMeasureName.equals(CosineSimilarity.class.getSimpleName())) {
                distanceMeasure = new CosineSimilarity();
            } else {
                distanceMeasure = new EuclideanDistance();
            }

            mdl.withCentroids(centers);
            mdl.withDistanceMeasure(distanceMeasure);
            return mdl;
        }
    }
}
