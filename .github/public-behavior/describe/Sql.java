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

import java.io.StringReader;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;

/**
 * Describes the keywords of the parser and the operators and functions that the Calcite engine uses. The kind and the allowed
 * signatures of an operator are part of the key, so the forms of an overloaded function are compared separately. Operators that
 * still share a key are counted, so removing one of them is a change. An operator registered twice is one form: the parser uses
 * the same object.
 */
class Sql implements Collector {
    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) {
        FrameworkConfig cfg = CalciteQueryProcessor.FRAMEWORK_CONFIG;
        SqlAbstractParserImpl parser = cfg.getParserConfig().parserFactory().getParser(new StringReader(""));

        for (String token : parser.getMetadata().getTokens()) {
            out.add("SQL keywords", "Calcite " + token, parser.getMetadata().isReservedWord(token) ? "reserved" : "non-reserved", "",
                tree.where(parser.getClass(), null, -1, false));
        }

        Map<String, Set<SqlOperator>> forms = new TreeMap<>();

        for (SqlOperator op : cfg.getOperatorTable().getOperatorList()) {
            if (op.getName().startsWith("$"))
                continue;

            String signatures;

            try {
                signatures = op.getAllowedSignatures();
            }
            catch (RuntimeException e) {
                signatures = "";
            }

            String key = Declarations.oneLine(op.getName() + ' ' + op.getSyntax() + ' ' + op.getKind() + ' ' + signatures);

            forms.computeIfAbsent(key, k -> Collections.newSetFromMap(new IdentityHashMap<>())).add(op);
        }

        String location = tree.where(CalciteQueryProcessor.class, "FRAMEWORK_CONFIG", -1, false);

        forms.forEach((key, ops) -> out.add("SQL functions", key, ops.size() > 1 ? ops.size() + " operators" : "", "", location));
    }
}
