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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteException;

/** */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RelJsonReader {
    private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
        new TypeReference<LinkedHashMap<String, Object>>() {};

    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;
    private final RelJson relJson = new RelJson();
    private final Map<String, RelNode> relMap = new LinkedHashMap<>();
    private RelNode lastRel;

    public RelJsonReader(RelOptCluster cluster, RelOptSchema relOptSchema) {
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
    }

    public RelNode read(String s) {
        try {
            lastRel = null;
            Map<String, Object> o = new ObjectMapper().readValue(s, TYPE_REF);
            List<Map<String, Object>> rels = (List)o.get("rels");
            readRels(rels);
            return lastRel;
        }
        catch (JsonProcessingException e) {
            throw new IgniteException(e);
        }
    }

    private void readRels(List<Map<String, Object>> jsonRels) {
        for (Map<String, Object> jsonRel : jsonRels)
            readRel(jsonRel);
    }

    private void readRel(Map<String, Object> jsonRel) {
        String id = (String)jsonRel.get("id");
        String type = (String)jsonRel.get("relOp");
        Function<RelInput, RelNode> factory = relJson.factory(type);
        RelNode rel = factory.apply(new RelInputImpl(jsonRel));
        relMap.put(id, rel);
        lastRel = rel;
    }

    private class RelInputImpl implements RelInput {
        private final Map<String, Object> jsonRel;

        private RelInputImpl(Map<String, Object> jsonRel) {
            this.jsonRel = jsonRel;
        }

        @Override public RelOptCluster getCluster() {
            return cluster;
        }

        @Override public RelTraitSet getTraitSet() {
            return cluster.traitSet();
        }

        @Override public RelOptTable getTable(String table) {
            List<String> list = getStringList(table);
            return relOptSchema.getTableForMember(list);
        }

        @Override public RelNode getInput() {
            List<RelNode> inputs = getInputs();
            assert inputs.size() == 1;
            return inputs.get(0);
        }

        @Override public List<RelNode> getInputs() {
            List<String> jsonInputs = getStringList("inputs");
            if (jsonInputs == null)
                return ImmutableList.of(lastRel);
            List<RelNode> inputs = new ArrayList<>();
            for (String jsonInput : jsonInputs)
                inputs.add(lookupInput(jsonInput));
            return inputs;
        }

        @Override public RexNode getExpression(String tag) {
            return relJson.toRex(this, jsonRel.get(tag));
        }

        @Override public ImmutableBitSet getBitSet(String tag) {
            return ImmutableBitSet.of(getIntegerList(tag));
        }

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

        @Override public List<String> getStringList(String tag) {
            return (List<String>)jsonRel.get(tag);
        }

        @Override public List<Integer> getIntegerList(String tag) {
            return (List<Integer>)jsonRel.get(tag);
        }

        @Override public List<List<Integer>> getIntegerListList(String tag) {
            return (List<List<Integer>>)jsonRel.get(tag);
        }

        @Override public List<AggregateCall> getAggregateCalls(String tag) {
            List<Map<String, Object>> jsonAggs = (List)jsonRel.get(tag);
            List<AggregateCall> inputs = new ArrayList<>();
            for (Map<String, Object> jsonAggCall : jsonAggs)
                inputs.add(toAggCall(jsonAggCall));
            return inputs;
        }

        @Override public Object get(String tag) {
            return jsonRel.get(tag);
        }

        @Override public String getString(String tag) {
            return (String)jsonRel.get(tag);
        }

        @Override public float getFloat(String tag) {
            return ((Number)jsonRel.get(tag)).floatValue();
        }

        @Override public boolean getBoolean(String tag, boolean default_) {
            Boolean b = (Boolean)jsonRel.get(tag);
            return b != null ? b : default_;
        }

        @Override public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
            return Util.enumVal(enumClass,
                getString(tag).toUpperCase(Locale.ROOT));
        }

        @Override public List<RexNode> getExpressionList(String tag) {
            List<Object> jsonNodes = (List)jsonRel.get(tag);
            List<RexNode> nodes = new ArrayList<>();
            for (Object jsonNode : jsonNodes)
                nodes.add(relJson.toRex(this, jsonNode));
            return nodes;
        }

        @Override public RelDataType getRowType(String tag) {
            Object o = jsonRel.get(tag);
            return relJson.toType(cluster.getTypeFactory(), o);
        }

        @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
            List<RexNode> expressionList = getExpressionList(expressionsTag);
            List<String> names =
                (List<String>)get(fieldsTag);
            return cluster.getTypeFactory().createStructType(
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

        @Override public RelCollation getCollation() {
            return relJson.toCollation((List)get("collation"));
        }

        @Override public RelDistribution getDistribution() {
            return relJson.toDistribution(get("distribution"));
        }

        @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
            List<List> jsonTuples = (List)get(tag);
            ImmutableList.Builder<ImmutableList<RexLiteral>> builder =
                ImmutableList.builder();
            for (List jsonTuple : jsonTuples)
                builder.add(getTuple(jsonTuple));
            return builder.build();
        }

        private RelNode lookupInput(String jsonInput) {
            RelNode node = relMap.get(jsonInput);
            if (node == null)
                throw new RuntimeException("unknown id " + jsonInput
                    + " for relational expression");
            return node;
        }

        private ImmutableList<RexLiteral> getTuple(List jsonTuple) {
            ImmutableList.Builder<RexLiteral> builder =
                ImmutableList.builder();
            for (Object jsonValue : jsonTuple)
                builder.add((RexLiteral)relJson.toRex(this, jsonValue));
            return builder.build();
        }

        private AggregateCall toAggCall(Map<String, Object> jsonAggCall) {
            Map<String, Object> aggMap = (Map)jsonAggCall.get("agg");
            SqlAggFunction aggregation = (SqlAggFunction)relJson.toOp(aggMap);
            Boolean distinct = (Boolean)jsonAggCall.get("distinct");
            List<Integer> operands = (List<Integer>)jsonAggCall.get("operands");
            Integer filterOperand = (Integer)jsonAggCall.get("filter");
            RelDataType type = relJson.toType(cluster.getTypeFactory(), jsonAggCall.get("type"));
            String name = (String)jsonAggCall.get("name");
            return AggregateCall.create(aggregation, distinct, false, false, operands,
                filterOperand == null ? -1 : filterOperand,
                RelCollations.EMPTY,
                type, name);
        }
    }
}
