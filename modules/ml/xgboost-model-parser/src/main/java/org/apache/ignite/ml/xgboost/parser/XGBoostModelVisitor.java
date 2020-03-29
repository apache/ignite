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

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link XGBoostModelParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface XGBoostModelVisitor<T> extends ParseTreeVisitor<T> {
    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgValue}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgValue(XGBoostModelParser.XgValueContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgHeader}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgHeader(XGBoostModelParser.XgHeaderContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgNode}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgNode(XGBoostModelParser.XgNodeContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgLeaf}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgLeaf(XGBoostModelParser.XgLeafContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgTree}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgTree(XGBoostModelParser.XgTreeContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgModel}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgModel(XGBoostModelParser.XgModelContext ctx);
}
