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

import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.ignite.ml.tree.DecisionTreeConditionalNode;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelBaseVisitor;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelParser;

/**
 * XGBoost tree visitor that parses tree.
 */
public class XGTreeVisitor extends XGBoostModelBaseVisitor<DecisionTreeNode> {
    /** Index of the root node. */
    private static final int ROOT_NODE_IDX = 0;

    /** Dictionary for matching column name and index. */
    private final Map<String, Integer> dict;

    /**
     * Constructs a new instance of tree visitor.
     *
     * @param dict Dictionary for matching column name and index.
     */
    public XGTreeVisitor(Map<String, Integer> dict) {
        this.dict = dict;
    }

    /** {@inheritDoc} */
    @Override public DecisionTreeNode visitXgTree(XGBoostModelParser.XgTreeContext ctx) {
        Map<Integer, DecisionTreeConditionalNode> splitNodes = new HashMap<>();
        Map<Integer, DecisionTreeLeafNode> leafNodes = new HashMap<>();

        for (XGBoostModelParser.XgNodeContext nodeCtx : ctx.xgNode()) {
            int idx = Integer.valueOf(nodeCtx.INT(0).getText());
            String featureName = nodeCtx.STRING().getText();
            double threshold = parseXgValue(nodeCtx.xgValue());

            splitNodes.put(idx, new DecisionTreeConditionalNode(dict.get(featureName), threshold, null, null, null));
        }

        for (XGBoostModelParser.XgLeafContext leafCtx : ctx.xgLeaf()) {
            int idx = Integer.valueOf(leafCtx.INT().getText());
            double val = parseXgValue(leafCtx.xgValue());

            leafNodes.put(idx, new DecisionTreeLeafNode(val));
        }

        for (XGBoostModelParser.XgNodeContext nodeCtx : ctx.xgNode()) {
            int idx = Integer.valueOf(nodeCtx.INT(0).getText());
            int yesIdx = Integer.valueOf(nodeCtx.INT(1).getText());
            int noIdx = Integer.valueOf(nodeCtx.INT(2).getText());
            int missIdx = Integer.valueOf(nodeCtx.INT(3).getText());

            DecisionTreeConditionalNode node = splitNodes.get(idx);

            node.setElseNode(splitNodes.containsKey(yesIdx) ? splitNodes.get(yesIdx) : leafNodes.get(yesIdx));
            node.setThenNode(splitNodes.containsKey(noIdx) ? splitNodes.get(noIdx) : leafNodes.get(noIdx));
            node.setMissingNode(splitNodes.containsKey(missIdx) ? splitNodes.get(missIdx) : leafNodes.get(missIdx));
        }

        return splitNodes.containsKey(ROOT_NODE_IDX) ? splitNodes.get(ROOT_NODE_IDX) : leafNodes.get(ROOT_NODE_IDX);
    }

    /**
     * Parses value (int of double).
     *
     * @param valCtx Value context.
     * @return Value.
     */
    private double parseXgValue(XGBoostModelParser.XgValueContext valCtx) {
        TerminalNode terminalNode = valCtx.INT() != null ? valCtx.INT() : valCtx.DOUBLE();

        return Double.valueOf(terminalNode.getText());
    }
}
