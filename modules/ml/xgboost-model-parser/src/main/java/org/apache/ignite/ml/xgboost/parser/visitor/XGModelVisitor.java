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
import java.util.List;
import org.apache.ignite.ml.xgboost.XGModel;
import org.apache.ignite.ml.xgboost.XGNode;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelBaseVisitor;
import org.apache.ignite.ml.xgboost.parser.XGBoostModelParser;

/**
 * XGBoost model visitor that parses model.
 */
public class XGModelVisitor extends XGBoostModelBaseVisitor<XGModel> {
    /** Tree visitor. */
    private final XGTreeVisitor treeVisitor = new XGTreeVisitor();

    /** {@inheritDoc} */
    @Override public XGModel visitXgModel(XGBoostModelParser.XgModelContext ctx) {
        List<XGNode> trees = new ArrayList<>();

        for (XGBoostModelParser.XgTreeContext treeCtx : ctx.xgTree())
            trees.add(treeVisitor.visitXgTree(treeCtx));

        return new XGModel(trees);
    }
}