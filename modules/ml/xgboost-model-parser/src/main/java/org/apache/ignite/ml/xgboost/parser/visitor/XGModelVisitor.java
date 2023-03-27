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

package org.apache.ignite.ml.xgboost.parser.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.xgboost.XGModelComposition;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelBaseVisitor;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelParser;

/**
 * XGBoost model visitor that parses model.
 */
public class XGModelVisitor extends XGBoostModelBaseVisitor<XGModelComposition> {
    /** Tree dictionary visitor. */
    private final XGTreeDictionaryVisitor treeDictionaryVisitor = new XGTreeDictionaryVisitor();

    /** {@inheritDoc} */
    @Override public XGModelComposition visitXgModel(XGBoostModelParser.XgModelContext ctx) {
        List<DecisionTreeNode> trees = new ArrayList<>();

        Set<String> featureNames = new HashSet<>();
        for (XGBoostModelParser.XgTreeContext treeCtx : ctx.xgTree())
            featureNames.addAll(treeDictionaryVisitor.visitXgTree(treeCtx));

        Map<String, Integer> dict = buildDictionary(featureNames);

        XGTreeVisitor treeVisitor = new XGTreeVisitor(dict);

        for (XGBoostModelParser.XgTreeContext treeCtx : ctx.xgTree()) {
            DecisionTreeNode treeNode = treeVisitor.visitXgTree(treeCtx);
            trees.add(treeNode);
        }

        return new XGModelComposition(dict, trees);
    }

    /**
     * Build dictionary using specified feature names.
     *
     * @param featureNames Feature names.
     * @return Dictionary.
     */
    private Map<String, Integer> buildDictionary(Set<String> featureNames) {
        List<String> orderedFeatureNames = new ArrayList<>(featureNames);
        Collections.sort(orderedFeatureNames);

        Map<String, Integer> dict = new HashMap<>();
        for (int i = 0; i < orderedFeatureNames.size(); i++)
            dict.put(orderedFeatureNames.get(i), i);

        return dict;
    }
}
