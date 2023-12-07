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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implementation class for {@link RelMetadataQuery#collations(RelNode)} method call.
 * See {@link org.apache.calcite.rel.metadata.RelMdCollation}
 */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.COLLATIONS.method, new IgniteMdCollation());

    //~ Constructors -----------------------------------------------------------

    /** */
    private IgniteMdCollation() {}

    //~ Methods ----------------------------------------------------------------

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.Collation> getDef() {
        return BuiltInMetadata.Collation.DEF;
    }

    /** Catch-all implementation for
     * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Collation#collations()},
     * invoked using reflection, for any relational expression not
     * handled by a more specific method.
     *
     * <p>{@link org.apache.calcite.rel.core.Union},
     * {@link org.apache.calcite.rel.core.Intersect},
     * {@link org.apache.calcite.rel.core.Minus},
     * {@link org.apache.calcite.rel.core.Join},
     * {@link org.apache.calcite.rel.core.Correlate}
     * do not in general return sorted results
     * (but implementations using particular algorithms may).
     *
     * @param rel Relational expression
     * @return Relational expression's collations
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#collations(RelNode)
     */
    public ImmutableList<RelCollation> collations(RelNode rel,
        RelMetadataQuery mq) {
        return ImmutableList.of();
    }

    /** */
    public ImmutableList<RelCollation> collations(
        IgniteRel rel,
        RelMetadataQuery mq
    ) {
        RelCollation collation = rel.collation();
        if (collation == null || F.isEmpty(collation.getFieldCollations()))
            return ImmutableList.of();

        return ImmutableList.of(collation);
    }

    /** */
    public ImmutableList<RelCollation> collations(Window rel,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(window(mq, rel.getInput(), rel.groups));
    }

    /** */
    public ImmutableList<RelCollation> collations(Match rel,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            match(mq, rel.getInput(), rel.getRowType(), rel.getPattern(),
                rel.isStrictStart(), rel.isStrictEnd(),
                rel.getPatternDefinitions(), rel.getMeasures(), rel.getAfter(),
                rel.getSubsets(), rel.isAllRows(), rel.getPartitionKeys(),
                rel.getOrderKeys(), rel.getInterval()));
    }

    /** */
    public ImmutableList<RelCollation> collations(Filter rel,
        RelMetadataQuery mq) {
        return mq.collations(rel.getInput());
    }

    /** */
    public ImmutableList<RelCollation> collations(TableModify rel,
        RelMetadataQuery mq) {
        return mq.collations(rel.getInput());
    }

    /** */
    public ImmutableList<RelCollation> collations(TableScan scan,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(table(scan.getTable()));
    }

    /** */
    public ImmutableList<RelCollation> collations(Sort sort,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            RelMdCollation.sort(sort.getCollation()));
    }

    /** */
    public ImmutableList<RelCollation> collations(SortExchange sort,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            RelMdCollation.sort(sort.getCollation()));
    }

    /** */
    public ImmutableList<RelCollation> collations(Project project,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            project(mq, project.getInput(), project.getProjects()));
    }

    /** */
    public ImmutableList<RelCollation> collations(Calc calc,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(calc(mq, calc.getInput(), calc.getProgram()));
    }

    /** */
    public ImmutableList<RelCollation> collations(Values values,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            values(mq, values.getRowType(), values.getTuples()));
    }

    /** */
    public ImmutableList<RelCollation> collations(JdbcToEnumerableConverter rel,
        RelMetadataQuery mq) {
        return mq.collations(rel.getInput());
    }

    /** */
    public ImmutableList<RelCollation> collations(HepRelVertex rel,
        RelMetadataQuery mq) {
        return mq.collations(rel.getCurrentRel());
    }

    /** */
    public ImmutableList<RelCollation> collations(RelSubset rel,
        RelMetadataQuery mq) {
        return ImmutableList.copyOf(
            Objects.requireNonNull(
                rel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));
    }

    // Helper methods

    /** Helper method to determine a
     * {@link org.apache.calcite.rel.core.TableScan}'s collation. */
    public static List<RelCollation> table(RelOptTable table) {
        return table.getCollationList();
    }

    /** Helper method to determine a
     * {@link org.apache.calcite.rel.core.Calc}'s collation. */
    public static List<RelCollation> calc(RelMetadataQuery mq, RelNode input,
        RexProgram program) {
        final List<RexNode> projects =
            program
                .getProjectList()
                .stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList());
        return project(mq, input, projects);
    }

    /** Helper method to determine a {@link Project}'s collation. */
    public static List<RelCollation> project(RelMetadataQuery mq,
        RelNode input, List<? extends RexNode> projects) {
        final SortedSet<RelCollation> collations = new TreeSet<>();
        final List<RelCollation> inputCollations = mq.collations(input);
        if (inputCollations == null || inputCollations.isEmpty()) {
            return ImmutableList.of();
        }
        final Multimap<Integer, Integer> targets = LinkedListMultimap.create();
        final Map<Integer, SqlMonotonicity> targetsWithMonotonicity =
            new HashMap<>();
        for (Ord<RexNode> project : Ord.<RexNode>zip(projects)) {
            if (project.e instanceof RexInputRef) {
                targets.put(((RexInputRef)project.e).getIndex(), project.i);
            }
            else if (project.e instanceof RexCall) {
                final RexCall call = (RexCall)project.e;
                final RexCallBinding binding =
                    RexCallBinding.create(Commons.typeFactory(input), call, inputCollations);
                targetsWithMonotonicity.put(project.i, call.getOperator().getMonotonicity(binding));
            }
        }
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
        loop:
        for (RelCollation ic : inputCollations) {
            if (ic.getFieldCollations().isEmpty()) {
                continue;
            }
            fieldCollations.clear();
            for (RelFieldCollation ifc : ic.getFieldCollations()) {
                final Collection<Integer> integers = targets.get(ifc.getFieldIndex());
                if (integers.isEmpty()) {
                    continue loop; // cannot do this collation
                }
                fieldCollations.add(ifc.withFieldIndex(integers.iterator().next()));
            }
            assert !fieldCollations.isEmpty();
            collations.add(RelCollations.of(fieldCollations));
        }

        final List<RelFieldCollation> fieldCollationsForRexCalls =
            new ArrayList<>();
        for (Map.Entry<Integer, SqlMonotonicity> entry
            : targetsWithMonotonicity.entrySet()) {
            final SqlMonotonicity value = entry.getValue();
            switch (value) {
                case NOT_MONOTONIC:
                case CONSTANT:
                    break;
                default:
                    fieldCollationsForRexCalls.add(
                        new RelFieldCollation(entry.getKey(),
                            RelFieldCollation.Direction.of(value)));
                    break;
            }
        }

        if (!fieldCollationsForRexCalls.isEmpty()) {
            collations.add(RelCollations.of(fieldCollationsForRexCalls));
        }

        return ImmutableList.copyOf(collations);
    }

    /** Helper method to determine a
     * {@link org.apache.calcite.rel.core.Window}'s collation.
     *
     * <p>A Window projects the fields of its input first, followed by the output
     * from each of its windows. Assuming (quite reasonably) that the
     * implementation does not re-order its input rows, then any collations of its
     * input are preserved. */
    public static List<RelCollation> window(RelMetadataQuery mq, RelNode input,
        ImmutableList<Window.Group> groups) {
        return mq.collations(input);
    }

    /** Helper method to determine a
     * {@link org.apache.calcite.rel.core.Match}'s collation. */
    public static List<RelCollation> match(RelMetadataQuery mq, RelNode input,
        RelDataType rowType, RexNode pattern,
        boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
        RexNode interval) {
        return mq.collations(input);
    }

    /** Helper method to determine a
     * {@link org.apache.calcite.rel.core.Values}'s collation.
     *
     * <p>We actually under-report the collations. A Values with 0 or 1 rows - an
     * edge case, but legitimate and very common - is ordered by every permutation
     * of every subset of the columns.
     *
     * <p>So, our algorithm aims to:<ul>
     *   <li>produce at most N collations (where N is the number of columns);
     *   <li>make each collation as long as possible;
     *   <li>do not repeat combinations already emitted -
     *       if we've emitted {@code (a, b)} do not later emit {@code (b, a)};
     *   <li>probe the actual values and make sure that each collation is
     *      consistent with the data
     * </ul>
     *
     * <p>So, for an empty Values with 4 columns, we would emit
     * {@code (a, b, c, d), (b, c, d), (c, d), (d)}. */
    public static List<RelCollation> values(RelMetadataQuery mq,
        RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples) {
        Util.discard(mq); // for future use
        final List<RelCollation> list = new ArrayList<>();
        final int n = rowType.getFieldCount();
        final List<Pair<RelFieldCollation, Ordering<List<RexLiteral>>>> pairs =
            new ArrayList<>();
        outer:
        for (int i = 0; i < n; i++) {
            pairs.clear();
            for (int j = i; j < n; j++) {
                final RelFieldCollation fieldCollation = new RelFieldCollation(j);
                Ordering<List<RexLiteral>> comparator = comparator(fieldCollation);
                Ordering<List<RexLiteral>> ordering;
                if (pairs.isEmpty()) {
                    ordering = comparator;
                }
                else {
                    ordering = Util.last(pairs).right.compound(comparator);
                }
                pairs.add(Pair.of(fieldCollation, ordering));
                if (!ordering.isOrdered(tuples)) {
                    if (j == i) {
                        continue outer;
                    }
                    pairs.remove(pairs.size() - 1);
                }
            }
            if (!pairs.isEmpty()) {
                list.add(RelCollations.of(Pair.left(pairs)));
            }
        }
        return list;
    }

    /** */
    private static Ordering<List<RexLiteral>> comparator(
        RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        switch (fieldCollation.direction) {
            case ASCENDING:
                return new Ordering<List<RexLiteral>>() {
                    public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
                        final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
                        final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
                        return RelFieldCollation.compare(c1, c2, nullComparison);
                    }
                };
            default:
                return new Ordering<List<RexLiteral>>() {
                    public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
                        final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
                        final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
                        return RelFieldCollation.compare(c2, c1, -nullComparison);
                    }
                };
        }
    }
}
