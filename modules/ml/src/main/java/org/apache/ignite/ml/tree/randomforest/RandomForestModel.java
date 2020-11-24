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

package org.apache.ignite.ml.tree.randomforest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.inference.JSONWritable;
import org.apache.ignite.ml.tree.randomforest.data.RandomForestTreeModel;

/**
 * Random Forest Model class.
 */
public class RandomForestModel extends ModelsComposition<RandomForestTreeModel> implements JSONWritable {
    /** Serial version uid. */
    private static final long serialVersionUID = 3476345240155508004L;

    /** */
    public RandomForestModel() {
        super(new ArrayList<>(), new MeanValuePredictionsAggregator());

    }

    /** */
    public RandomForestModel(List<RandomForestTreeModel> oldModels, PredictionsAggregator predictionsAggregator) {
        super(oldModels, predictionsAggregator);
    }

    /**
     * Returns predictions aggregator.
     */
    @Override public PredictionsAggregator getPredictionsAggregator() {
        return predictionsAggregator;
    }

    /**
     * Returns containing models.
     */
    @Override public List<RandomForestTreeModel> getModels() {
        return models;
    }

    public static RandomForestModel fromJSON(Path path) {
            ObjectMapper mapper = new ObjectMapper();

            RandomForestModel mdl;
            try {
                mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), RandomForestModel.class);

                return mdl;
            } catch (IOException e) {
                e.printStackTrace();
            }
        return null;
    }
}
