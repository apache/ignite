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

package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point to extend current sql engine with external storage or even custom execution.
 */
public interface SqlExtension {
    /**
     * Returns the name of the current extension.
     *
     * <p>This name will be used to distinguish between different
     * extensions. Also the {@link CatalogUpdateListener} will register
     * provided catalog with the same name.
     *
     * @return Name of the extension.
     */
    String name();

    /**
     * Initializes the extension before use.
     *
     * @param ignite Instance of the current Ignite node.
     * @param catalogUpdateListener Listener to notify when new table or schema
     *                              are available. Note: the catalog listener creates
     *                              copy of the provided catalog, so its not enough
     *                              to just update {@link ExternalCatalog catalog} or
     *                              {@link ExternalSchema schema}, the listener should
     *                              be called explicitly to register changes.
     * @see ExternalSchema
     * @see ExternalCatalog
     */
    void init(
            Ignite ignite,
            CatalogUpdateListener catalogUpdateListener
    );

    /**
     * Returns a set of optimization rules for given optimization phase.
     *
     * @param phase Current optimization phase.
     * @return Set of rules, or empty set if there are no rules for given phase.
     */
    Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase);
    
    /**
     * Returns an implementor of relations provided by current extension.
     *
     * @param <RowT> Type of the rows used in current runtime.
     * @return Implementor of the relations provided by current extension.
     * @see org.apache.ignite.internal.processors.query.calcite.exec.RowHandler
     * @see org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory
     * @see RelImplementor
     */
    <RowT> RelImplementor<RowT> implementor();
    
    /**
     * Returns colocation group for given relational tree.
     *
     * <p>It's guaranteed that this tree will only consist of the relations
     * provided by the current extension.
     *
     * @param node Relational tree to find colocation for.
     * @return Colocation of given relation tree.
     */
    ColocationGroup colocationGroup(IgniteRel node);
    
    /**
     * Implementer to create execution nodes from provided relational nodes.
     *
     * <p>Should provide implementation for every physical node introduced by current extension.
     *
     * @param <RowT> Type of the rows used in current runtime.
     * @see org.apache.ignite.internal.processors.query.calcite.exec.RowHandler
     * @see org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory
     */
    interface RelImplementor<RowT> {
        /**
         * Converts given relational tree into an execution tree.
         *
         * <p>It's guaranteed that the tree will only consist of the relations
         * provided by the current extension.
         *
         * @param ctx An execution context.
         * @param node A root of the relational tree.
         * @return A root of the resulting execution tree.
         */
        Node<RowT> implement(ExecutionContext<RowT> ctx, IgniteRel node);
    }
    
    /**
     * Represents an external SQL schema that is simply a group different tables.
     */
    interface ExternalSchema {
        /** Returns list of all tables provided by current schema. */
        List<String> tableNames();
    
        /**
         * Returns table by its name.
         *
         * @param name Name of the table.
         * @return The table, or {@code null} if there is no table with given name.
         */
        @Nullable RelOptTable table(String name);
    }
    
    /**
     * Represents an external SQL catalog that is simply a group different schemas.
     */
    interface ExternalCatalog {
        /** Returns list of all schemas provided by current catalog. */
        List<String> schemaNames();
    
        /**
         * Returns schema by its name.
         *
         * @param name Name of the schema.
         * @return The schema, or {@code null} if there is no schema with given name.
         */
        @Nullable ExternalSchema schema(String name);
    }
}
