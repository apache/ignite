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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.ignite.ml.inference.parser.InfModelParser;
import org.apache.ignite.ml.xgboost.XGModel;
import org.apache.ignite.ml.xgboost.XGObject;
import org.apache.ignite.ml.xgboost.parser.visitor.XGModelVisitor;

/** XGBoost model parser. */
public class XGModelParser implements InfModelParser<XGObject, Double> {
    /** */
    private static final long serialVersionUID = -5819843559270294718L;

    /** {@inheritDoc} */
    @Override public XGModel parse(byte[] mdl) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(mdl)) {
            CharStream cStream = CharStreams.fromStream(bais);
            XGBoostModelLexer lexer = new XGBoostModelLexer(cStream);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            XGBoostModelParser parser = new XGBoostModelParser(tokens);

            XGModelVisitor visitor = new XGModelVisitor();

            return visitor.visit(parser.xgModel());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
