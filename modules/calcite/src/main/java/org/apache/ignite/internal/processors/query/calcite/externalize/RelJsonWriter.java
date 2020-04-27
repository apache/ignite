/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.externalize;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteException;

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class RelJsonWriter implements RelWriter {
    private final RelJson relJson = new RelJson();
    private final List<Object> relList = new ArrayList<>();
    private final Map<RelNode, String> relIdMap = new IdentityHashMap<>();

    private final boolean pretty;

    private String previousId;
    private List<Pair<String, Object>> items = new ArrayList<>();

    public RelJsonWriter() {
        this(false);
    }

    public RelJsonWriter(boolean pretty) {
        this.pretty = pretty;
    }

    @Override public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        try (RexNode.Closeable ignored = withRexNormalize()) {
            explain_(rel, valueList);
        }
    }

    @Override public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    @Override public RelWriter item(String term, Object value) {
        items.add(Pair.of(term, value));
        return this;
    }

    @Override public RelWriter done(RelNode node) {
        List<Pair<String, Object>> current0 = items;
        items = new ArrayList<>();
        explain_(node, current0);
        return this;
    }

    @Override public boolean nest() {
        return true;
    }

    /** */
    public String asString() {
        try (RexNode.Closeable ignored = withRexNormalize()) {
            StringWriter writer = new StringWriter();
            ObjectMapper mapper = new ObjectMapper();

            ObjectWriter writer0 = pretty
                ? mapper.writer(new DefaultPrettyPrinter())
                : mapper.writer();

            writer0
                .withRootName("rels")
                .writeValue(writer, relList);

            return writer.toString();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    private void explain_(RelNode rel, List<Pair<String, Object>> values) {
        final Map<String, Object> map = relJson.map();

        map.put("id", null); // ensure that id is the first attribute
        map.put("relOp", relJson.classToTypeName(rel.getClass()));

        for (Pair<String, Object> value : values) {
            if (value.right instanceof RelNode)
                continue;

            map.put(value.left, relJson.toJson(value.right));
        }
        // omit 'inputs: ["3"]' if "3" is the preceding rel
        final List<Object> list = explainInputs(rel.getInputs());
        if (list.size() != 1 || !list.get(0).equals(previousId)) {
            map.put("inputs", list);
        }

        final String id = Integer.toString(relIdMap.size());
        relIdMap.put(rel, id);
        map.put("id", id);

        relList.add(map);
        previousId = id;
    }

    private List<Object> explainInputs(List<RelNode> inputs) {
        final List<Object> list = relJson.list();
        for (RelNode input : inputs) {
            String id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = previousId;
            }
            list.add(id);
        }
        return list;
    }
}
