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

package org.apache.ignite.internal.sql.engine.extension;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.FilterNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.PlannerPhase;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.jetbrains.annotations.Nullable;

/**
 * A test extension implementation.
 */
public class TestExtension implements SqlExtension {
    private static final String EXTENSION_NAME = "TEST_EXT";

    private static final String TEST_TABLE_NAME = "TEST_TBL";

    private static final String TEST_SCHEMA_NAME = "CUSTOM_SCHEMA";

    static final Convention CONVENTION = new ExternalConvention(EXTENSION_NAME, IgniteRel.class);

    public static List<String> allNodes;

    /** {@inheritDoc} */
    @Override
    public String name() {
        return EXTENSION_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public void init(CatalogUpdateListener catalogUpdateListener) {
        catalogUpdateListener.onCatalogUpdated(new ExternalCatalogImpl());
    }

    /** {@inheritDoc} */
    @Override
    public Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase) {
        if (phase != PlannerPhase.OPTIMIZATION) {
            return Set.of();
        }

        return Set.of(TestFilterConverterRule.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RelImplementor<RowT> implementor() {
        return new RelImplementor<>() {
            @Override
            public Node<RowT> implement(ExecutionContext<RowT> ctx, IgniteRel node) {
                if (node instanceof TestPhysFilter) {
                    return implement(ctx, (TestPhysFilter) node);
                }

                if (node instanceof TestPhysTableScan) {
                    return implement(ctx, (TestPhysTableScan) node);
                }

                throw new AssertionError("Unexpected node " + (node != null ? "of class " + node.getClass().getName() : "null"));
            }

            private Node<RowT> implement(ExecutionContext<RowT> ctx, TestPhysTableScan scan) {
                RowHandler.RowFactory<RowT> factory = ctx.rowHandler().factory(ctx.getTypeFactory(), scan.getRowType());

                return new ScanNode<>(
                        ctx, scan.getRowType(), List.of(
                        factory.create(ctx.localNodeId(), 1, UUID.randomUUID().toString()),
                        factory.create(ctx.localNodeId(), 2, UUID.randomUUID().toString()),
                        factory.create(ctx.localNodeId(), 3, UUID.randomUUID().toString())
                )
                );
            }

            private Node<RowT> implement(ExecutionContext<RowT> ctx, TestPhysFilter filter) {
                Predicate<RowT> pred = ctx.expressionFactory().predicate(filter.getCondition(), filter.getRowType());

                FilterNode<RowT> node = new FilterNode<>(ctx, filter.getRowType(), pred);

                Node<RowT> input = implement(ctx, (IgniteRel) filter.getInput());

                node.register(input);

                return node;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(IgniteRel node) {
        return ColocationGroup.forNodes(allNodes);
    }

    private static class ExternalCatalogImpl implements ExternalCatalog {
        private final Map<String, ExternalSchema> schemas = Map.of(TEST_SCHEMA_NAME, new ExternalSchemaImpl());

        /** {@inheritDoc} */
        @Override
        public List<String> schemaNames() {
            return List.of(TEST_SCHEMA_NAME);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable ExternalSchema schema(String name) {
            return schemas.get(name);
        }
    }

    private static class ExternalSchemaImpl implements ExternalSchema {
        private final Map<String, IgniteTable> tables = Map.of(TEST_TABLE_NAME, new TestTableImpl(
                new TableDescriptorImpl(
                        List.of(
                                new ColumnDescriptorImpl("NODE_ID", true, 0, NativeTypes.stringOf(256), null),
                                new ColumnDescriptorImpl("NUM", true, 1, NativeTypes.INT32, null),
                                new ColumnDescriptorImpl("VAL", false, 2, NativeTypes.stringOf(256), null)
                        )
                )
        ));

        /** {@inheritDoc} */
        @Override
        public List<String> tableNames() {
            return List.of(TEST_TABLE_NAME);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable IgniteTable table(String name) {
            return tables.get(name);
        }
    }
}
