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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RelJsonReader {
    /** */
    private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
        new TypeReference<LinkedHashMap<String, Object>>() {};

    /** */
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /** */
    private final RelOptSchema relOptSchema;

    /** */
    private final RelJson relJson;

    /** */
    private final Map<String, RelNode> relMap = new LinkedHashMap<>();

    /** */
    private RelNode lastRel;

    /** */
    public static <T extends RelNode> T fromJson(BaseQueryContext ctx, String json) {
        RelJsonReader reader = new RelJsonReader(ctx);

        return (T)reader.read(json);
    }

    /** */
    public RelJsonReader(BaseQueryContext qctx) {
        relOptSchema = qctx.catalogReader();

        relJson = new RelJson(qctx);
    }

    /** */
    public RelNode read(String s) {
        try {
            lastRel = null;
            Map<String, Object> o = mapper.readValue(s, TYPE_REF);
            List<Map<String, Object>> rels = (List)o.get("rels");
            readRels(rels);
            return lastRel;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void readRels(List<Map<String, Object>> jsonRels) {
        for (Map<String, Object> jsonRel : jsonRels)
            readRel(jsonRel);
    }

    /** */
    private void readRel(Map<String, Object> jsonRel) {
        String id = (String)jsonRel.get("id");
        String type = (String)jsonRel.get("relOp");
        Function<RelInput, RelNode> factory = relJson.factory(type);
        RelNode rel = factory.apply(new RelInputImpl(jsonRel));
        relMap.put(id, rel);
        lastRel = rel;
    }

    /** */
    private class RelInputImpl implements RelInputEx {
        /** */
        private final Map<String, Object> jsonRel;

        /** */
        private RelInputImpl(Map<String, Object> jsonRel) {
            this.jsonRel = jsonRel;
        }

        /** {@inheritDoc} */
        @Override public RelOptCluster getCluster() {
            return Commons.emptyCluster();
        }

        /** {@inheritDoc} */
        @Override public RelTraitSet getTraitSet() {
            return Commons.emptyCluster().traitSet();
        }

        /** {@inheritDoc} */
        @Override public RelOptTable getTable(String table) {
            List<String> list = getStringList(table);
            return relOptSchema.getTableForMember(list);
        }

        /** {@inheritDoc} */
        @Override public RelNode getInput() {
            List<RelNode> inputs = getInputs();
            assert inputs.size() == 1;
            return inputs.get(0);
        }

        /** {@inheritDoc} */
        @Override public List<RelNode> getInputs() {
            List<String> jsonInputs = getStringList("inputs");
            if (jsonInputs == null)
                return ImmutableList.of(lastRel);
            List<RelNode> inputs = new ArrayList<>();
            for (String jsonInput : jsonInputs)
                inputs.add(lookupInput(jsonInput));
            return inputs;
        }

        /** {@inheritDoc} */
        @Override public RexNode getExpression(String tag) {
            return relJson.toRex(this, jsonRel.get(tag));
        }

        /** {@inheritDoc} */
        @Override public ImmutableBitSet getBitSet(String tag) {
            return ImmutableBitSet.of(getIntegerList(tag));
        }

        /** {@inheritDoc} */
        @Override public List<ImmutableBitSet> getBitSetList(String tag) {
            List<List<Integer>> list = getIntegerListList(tag);
            if (list == null)
                return null;
            ImmutableList.Builder<ImmutableBitSet> builder =
                ImmutableList.builder();
            for (List<Integer> integers : list)
                builder.add(ImmutableBitSet.of(integers));
            return builder.build();
        }

        /** {@inheritDoc} */
        @Override public List<String> getStringList(String tag) {
            return (List<String>)jsonRel.get(tag);
        }

        /** {@inheritDoc} */
        @Override public List<Integer> getIntegerList(String tag) {
            return (List<Integer>)jsonRel.get(tag);
        }

        /** {@inheritDoc} */
        @Override public List<List<Integer>> getIntegerListList(String tag) {
            return (List<List<Integer>>)jsonRel.get(tag);
        }

        /** {@inheritDoc} */
        @Override public List<AggregateCall> getAggregateCalls(String tag) {
            List<Map<String, Object>> jsonAggs = (List)jsonRel.get(tag);
            List<AggregateCall> inputs = new ArrayList<>();
            for (Map<String, Object> jsonAggCall : jsonAggs)
                inputs.add(toAggCall(jsonAggCall));
            return inputs;
        }

        /** {@inheritDoc} */
        @Override public Object get(String tag) {
            return jsonRel.get(tag);
        }

        /** {@inheritDoc} */
        @Override public String getString(String tag) {
            return (String)jsonRel.get(tag);
        }

        /** {@inheritDoc} */
        @Override public float getFloat(String tag) {
            return ((Number)jsonRel.get(tag)).floatValue();
        }

        /** {@inheritDoc} */
        @Override public BigDecimal getBigDecimal(String tag) {
            return SqlFunctions.toBigDecimal(jsonRel.get(tag));
        }

        /** {@inheritDoc} */
        @Override public boolean getBoolean(String tag, boolean default_) {
            Boolean b = (Boolean)jsonRel.get(tag);
            return b != null ? b : default_;
        }

        /** {@inheritDoc} */
        @Override public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
            Object name = get(tag);
            if (name instanceof String) {
                // Some types of nodes (Join for joinType enum, for example) serialize names in lower case.
                E res = Util.enumVal(enumClass, ((String)name).toUpperCase(Locale.ROOT));

                if (res != null)
                    return res;
            }

            return relJson.toEnum(name);
        }

        /** {@inheritDoc} */
        @Override public List<RexNode> getExpressionList(String tag) {
            List<Object> jsonNodes = (List)jsonRel.get(tag);
            if (jsonNodes == null)
                return null;

            List<RexNode> nodes = new ArrayList<>();
            for (Object jsonNode : jsonNodes)
                nodes.add(relJson.toRex(this, jsonNode));
            return nodes;
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(String tag) {
            Object o = jsonRel.get(tag);
            return relJson.toType(Commons.typeFactory(Commons.emptyCluster()), o);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
            List<RexNode> expressionList = getExpressionList(expressionsTag);
            List<String> names =
                (List<String>)get(fieldsTag);
            return Commons.typeFactory(Commons.emptyCluster()).createStructType(
                new AbstractList<Map.Entry<String, RelDataType>>() {
                    @Override public Map.Entry<String, RelDataType> get(int index) {
                        return Pair.of(names.get(index),
                            expressionList.get(index).getType());
                    }

                    @Override public int size() {
                        return names.size();
                    }
                });
        }

        /** {@inheritDoc} */
        @Override public RelCollation getCollation() {
            return relJson.toCollation((List)get("collation"));
        }

        /** {@inheritDoc} */
        @Override public RelCollation getCollation(String tag) {
            return relJson.toCollation((List)get(tag));
        }

        /** {@inheritDoc} */
        @Override public List<SearchBounds> getSearchBounds(String tag) {
            return relJson.toSearchBoundList(this, (List<Map<String, Object>>)get(tag));
        }

        /** {@inheritDoc} */
        @Override public RelDistribution getDistribution() {
            return relJson.toDistribution(get("distribution"));
        }

        /** {@inheritDoc} */
        @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
            List<List> jsonTuples = (List)get(tag);
            ImmutableList.Builder<ImmutableList<RexLiteral>> builder =
                ImmutableList.builder();
            for (List jsonTuple : jsonTuples)
                builder.add(getTuple(jsonTuple));
            return builder.build();
        }

        /** */
        private RelNode lookupInput(String jsonInput) {
            RelNode node = relMap.get(jsonInput);
            if (node == null)
                throw new RuntimeException("unknown id " + jsonInput
                    + " for relational expression");
            return node;
        }

        /** */
        private ImmutableList<RexLiteral> getTuple(List jsonTuple) {
            ImmutableList.Builder<RexLiteral> builder =
                ImmutableList.builder();
            for (Object jsonVal : jsonTuple)
                builder.add((RexLiteral)relJson.toRex(this, jsonVal));
            return builder.build();
        }

        /** */
        private AggregateCall toAggCall(Map<String, Object> jsonAggCall) {
            Map<String, Object> aggMap = (Map)jsonAggCall.get("agg");
            SqlAggFunction aggregation = (SqlAggFunction)relJson.toOp(aggMap);
            Boolean distinct = (Boolean)jsonAggCall.get("distinct");
            List<Integer> operands = (List<Integer>)jsonAggCall.get("operands");
            Integer filterOperand = (Integer)jsonAggCall.get("filter");
            RelDataType type = relJson.toType(Commons.typeFactory(), jsonAggCall.get("type"));
            String name = (String)jsonAggCall.get("name");
            RelCollation collation = relJson.toCollation((List<Map<String, Object>>)jsonAggCall.get("coll"));
            List<RexNode> rexList = Commons.transform((List<Object>)jsonAggCall.get("rexList"),
                node -> relJson.toRex(this, node));

            return AggregateCall.create(aggregation, distinct, false, false, rexList, operands,
                filterOperand == null ? -1 : filterOperand, null, collation, type, name);
        }
    }
}
