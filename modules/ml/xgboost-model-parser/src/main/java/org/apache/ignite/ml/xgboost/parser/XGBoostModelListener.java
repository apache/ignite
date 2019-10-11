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

package org.apache.ignite.ml.xgboost.parser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link XGBoostModelParser}.
 */
public interface XGBoostModelListener extends ParseTreeListener {
    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgValue}.
     * @param ctx the parse tree
     */
    public void enterXgValue(XGBoostModelParser.XgValueContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgValue}.
     * @param ctx the parse tree
     */
    public void exitXgValue(XGBoostModelParser.XgValueContext ctx);

    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgHeader}.
     * @param ctx the parse tree
     */
    public void enterXgHeader(XGBoostModelParser.XgHeaderContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgHeader}.
     * @param ctx the parse tree
     */
    public void exitXgHeader(XGBoostModelParser.XgHeaderContext ctx);

    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgNode}.
     * @param ctx the parse tree
     */
    public void enterXgNode(XGBoostModelParser.XgNodeContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgNode}.
     * @param ctx the parse tree
     */
    public void exitXgNode(XGBoostModelParser.XgNodeContext ctx);

    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgLeaf}.
     * @param ctx the parse tree
     */
    public void enterXgLeaf(XGBoostModelParser.XgLeafContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgLeaf}.
     * @param ctx the parse tree
     */
    public void exitXgLeaf(XGBoostModelParser.XgLeafContext ctx);

    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgTree}.
     * @param ctx the parse tree
     */
    public void enterXgTree(XGBoostModelParser.XgTreeContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgTree}.
     * @param ctx the parse tree
     */
    public void exitXgTree(XGBoostModelParser.XgTreeContext ctx);

    /**
     * Enter a parse tree produced by {@link XGBoostModelParser#xgModel}.
     * @param ctx the parse tree
     */
    public void enterXgModel(XGBoostModelParser.XgModelContext ctx);

    /**
     * Exit a parse tree produced by {@link XGBoostModelParser#xgModel}.
     * @param ctx the parse tree
     */
    public void exitXgModel(XGBoostModelParser.XgModelContext ctx);
}
