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

package org.apache.ignite.ml.tree;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONModelMixIn;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.inference.json.JacksonHelper;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Base class for decision tree models.
 */
public class DecisionTreeModel implements IgniteModel<Vector, Double>, JSONWritable {
    /** Root node. */
    private DecisionTreeNode rootNode;

    /**
     * Creates the model.
     *
     * @param rootNode Root node of the tree.
     */
    public DecisionTreeModel(DecisionTreeNode rootNode) {
        this.rootNode = rootNode;
    }

    /** */
    private DecisionTreeModel() {

    }

    /** Returns the root node. */
    public DecisionTreeNode getRootNode() {
        return rootNode;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector features) {
        return rootNode.predict(features);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return DecisionTreeTrainer.printTree(rootNode, pretty);
    }

    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.addMixIn(DecisionTreeModel.class, JSONModelMixIn.class);

        ObjectWriter writer = mapper
            .writerFor(DecisionTreeModel.class)
            .withAttribute("formatVersion", JSONModel.JSON_MODEL_FORMAT_VERSION)
            .withAttribute("timestamp", System.currentTimeMillis())
            .withAttribute("uid", "dt_" + UUID.randomUUID().toString())
            .withAttribute("modelClass", DecisionTreeModel.class.getSimpleName());

        try {
            File file = new File(path.toAbsolutePath().toString());
            writer.writeValue(file, this);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Loads DecisionTreeModel from JSON file. */
    public static DecisionTreeModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        DecisionTreeModel mdl;
        try {
            JacksonHelper.readAndValidateBasicJsonModelProperties(path, mapper, DecisionTreeModel.class.getSimpleName());
            mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), DecisionTreeModel.class);
            return mdl;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
