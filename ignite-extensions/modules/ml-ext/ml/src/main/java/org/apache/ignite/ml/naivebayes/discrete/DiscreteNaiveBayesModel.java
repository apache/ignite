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

package org.apache.ignite.ml.naivebayes.discrete;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONModelMixIn;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.inference.json.JacksonHelper;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.BayesModel;

/**
 * Discrete naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as
 * {@code p(C_k,y) =x_1*p_k1^x *...*x_i*p_ki^x_i}. Where {@code x_i} is a discrete feature, {@code p_ki} is a prior
 * probability probability of class {@code p(x|C_k)}. Returns the number of the most possible class.
 */
public class DiscreteNaiveBayesModel implements BayesModel<DiscreteNaiveBayesModel, Vector, Double>,
    JSONWritable, DeployableObject {
    /** Serial version uid. */
    private static final long serialVersionUID = -127386523291350345L;

    /**
     * Probabilities of features for all classes for each label. {@code labels[c][f][b]} contains a probability for
     * class {@code c} for feature {@code f} for bucket {@code b}.
     */
    private double[][][] probabilities;

    /** Prior probabilities of each class */
    private double[] clsProbabilities;

    /** Labels. */
    private double[] labels;

    /**
     * The bucket thresholds to convert a features to discrete values. {@code bucketThresholds[f][b]} contains the right
     * border for feature {@code f} for bucket {@code b}. Everything which is above the last thresdold goes to the next
     * bucket.
     */
    private double[][] bucketThresholds;

    /** Amount values in each buckek for each feature per label. */
    private DiscreteNaiveBayesSumsHolder sumsHolder;

    /**
     * @param probabilities Probabilities of features for classes.
     * @param clsProbabilities Prior probabilities for classes.
     * @param labels Labels.
     * @param bucketThresholds The threshold to convert a feature to a binary value.
     * @param sumsHolder Amount values which are abouve the threshold per label.
     */
    public DiscreteNaiveBayesModel(double[][][] probabilities, double[] clsProbabilities, double[] labels,
        double[][] bucketThresholds, DiscreteNaiveBayesSumsHolder sumsHolder) {
        this.probabilities = probabilities.clone();
        this.clsProbabilities = clsProbabilities.clone();
        this.labels = labels.clone();
        this.bucketThresholds = bucketThresholds.clone();
        this.sumsHolder = sumsHolder;
    }

    /** */
    public DiscreteNaiveBayesModel() {
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<DiscreteNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /**
     * @param vector features vector.
     * @return A label with max probability.
     */
    @Override public Double predict(Vector vector) {
        double[] probapilityPowers = probabilityPowers(vector);

        int maxLbIdx = 0;
        for (int i = 0; i < probapilityPowers.length; i++) {
            probapilityPowers[i] += Math.log(clsProbabilities[i]);

            if (probapilityPowers[i] > probapilityPowers[maxLbIdx])
                maxLbIdx = i;
        }

        return labels[maxLbIdx];
    }

    /** {@inheritDoc} */
    @Override public double[] probabilityPowers(Vector vector) {
        double[] probapilityPowers = new double[clsProbabilities.length];

        for (int i = 0; i < clsProbabilities.length; i++) {
            for (int j = 0; j < probabilities[0].length; j++) {
                int x = toBucketNumber(vector.get(j), bucketThresholds[j]);
                double p = probabilities[i][j][x];
                probapilityPowers[i] += (p > 0 ? Math.log(p) : .0);
            }
        }

        return probapilityPowers;
    }

    /** A getter for probabilities.*/
    public double[][][] getProbabilities() {
        return probabilities.clone();
    }

    /** A getter for clsProbabilities.*/
    public double[] getClsProbabilities() {
        return clsProbabilities.clone();
    }

    /** A getter for bucketThresholds.*/
    public double[][] getBucketThresholds() {
        return bucketThresholds.clone();
    }

    /** A getter for labels.*/
    public double[] getLabels() {
        return labels.clone();
    }

    /** A getter for sumsHolder.*/
    public DiscreteNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    /** Returs a bucket number to which the {@code value} corresponds. */
    private int toBucketNumber(double val, double[] thresholds) {
        for (int i = 0; i < thresholds.length; i++) {
            if (val < thresholds[i])
                return i;
        }

        return thresholds.length;
    }

    /** {@inheritDoc} */
    @JsonIgnore
    @Override public List<Object> getDependencies() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.addMixIn(DiscreteNaiveBayesModel.class, JSONModelMixIn.class);

        ObjectWriter writer = mapper
            .writerFor(DiscreteNaiveBayesModel.class)
            .withAttribute("formatVersion", JSONModel.JSON_MODEL_FORMAT_VERSION)
            .withAttribute("timestamp", System.currentTimeMillis())
            .withAttribute("uid", "dt_" + UUID.randomUUID().toString())
            .withAttribute("modelClass", DiscreteNaiveBayesModel.class.getSimpleName());

        try {
            File file = new File(path.toAbsolutePath().toString());
            writer.writeValue(file, this);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Loads DiscreteNaiveBayesModel from JSON file. */
    public static DiscreteNaiveBayesModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        DiscreteNaiveBayesModel mdl;
        try {
            JacksonHelper.readAndValidateBasicJsonModelProperties(path, mapper, DiscreteNaiveBayesModel.class.getSimpleName());
            mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), DiscreteNaiveBayesModel.class);
            return mdl;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
