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
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * This class encapsulates result of clusterization by KMeans algorithm.
 */
public final class KMeansModel implements ClusterizationModel<Vector, Integer>, Exportable<KMeansModelFormat>,
    JSONWritable, DeployableObject {
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
    private KMeansModel() {

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
        List<String> centersList = Arrays.stream(centers).map(x -> Tracer.asAscii(x, "%.4f", false))
            .collect(Collectors.toList());

        return ModelTrace.builder("KMeansModel", pretty)
            .addField("distance measure", distanceMeasure.toString())
            .addField("centroids", centersList)
            .toString();
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(distanceMeasure);
    }

    /** Loads KMeansModel from JSON file. */
    public static KMeansModel fromJSON(Path path) {
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
                KMeansJSONExportModel exportModel = new KMeansJSONExportModel(
                    System.currentTimeMillis(),
                    "ann_" + UUID.randomUUID().toString(),
                    KMeansModel.class.getSimpleName()
                );
                List<double[]> listOfCenters = new ArrayList<>();
                for (int i = 0; i < centers.length; i++) {
                    listOfCenters.add(centers[i].asArray());
                }

                exportModel.mdlCenters = listOfCenters;
                exportModel.distanceMeasure = distanceMeasure;

                File file = new File(path.toAbsolutePath().toString());
                mapper.writeValue(file, exportModel);
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /** */
    public static class KMeansJSONExportModel extends JSONModel {
        /** Centers of clusters. */
        public List<double[]> mdlCenters;

        /** Distance measure. */
        public DistanceMeasure distanceMeasure;

        /** */
        public KMeansJSONExportModel(Long timestamp, String uid, String modelClass) {
            super(timestamp, uid, modelClass);
        }

        /** */
        @JsonCreator
        public KMeansJSONExportModel() {
        }

        /** {@inheritDoc} */
        @Override public KMeansModel convert() {
            KMeansModel mdl = new KMeansModel();
            Vector[] centers = new DenseVector[mdlCenters.size()];
            for (int i = 0; i < mdlCenters.size(); i++) {
                centers[i] = VectorUtils.of(mdlCenters.get(i));
            }

            DistanceMeasure distanceMeasure = this.distanceMeasure;

            mdl.withCentroids(centers);
            mdl.withDistanceMeasure(distanceMeasure);
            return mdl;
        }
    }
}
