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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.PARTITIONS;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.REPLICAS;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOption;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteException;

/**
 * DdlSqlToCommandConverter. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class DdlSqlToCommandConverter {
    /** Processor that unconditionally throws an AssertionException. */
    private static final TableOptionProcessor<Void> UNSUPPORTED_OPTION_PROCESSOR = new TableOptionProcessor<>(
            null,
            (opt, ctx) -> {
                throw new AssertionError("Unsupported option " + opt.key());
            },
            null);

    /** Checks positive num param. */
    private BiFunction<IgniteSqlCreateTableOption, PlanningContext, Integer> positiveNumValidator = (opt, ctx) -> {
        if (!(opt.value() instanceof SqlNumericLiteral)
                || !((SqlNumericLiteral) opt.value()).isInteger()
                || ((SqlLiteral) opt.value()).intValue(true) < 0
        ) {
            throwOptionParsingException(opt, "a non-negative integer", ctx.query());
        }

        return ((SqlLiteral) opt.value()).intValue(true);
    };

    /** Map of the supported table option processors. */
    private final Map<IgniteSqlCreateTableOptionEnum, TableOptionProcessor<?>> tblOptionProcessors = Stream.of(
            new TableOptionProcessor<>(REPLICAS, positiveNumValidator, CreateTableCommand::replicas),
            new TableOptionProcessor<>(PARTITIONS, positiveNumValidator, CreateTableCommand::partitions)
    ).collect(Collectors.toMap(TableOptionProcessor::key, Function.identity()));

    /**
     * Converts a given ddl AST to a ddl command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx     Planning context.
     */
    public DdlCommand convert(SqlDdl ddlNode, PlanningContext ctx) {
        if (ddlNode instanceof IgniteSqlCreateTable) {
            return convertCreateTable((IgniteSqlCreateTable) ddlNode, ctx);
        }

        if (ddlNode instanceof SqlDropTable) {
            return convertDropTable((SqlDropTable) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableAddColumn) {
            return convertAlterTableAdd((IgniteSqlAlterTableAddColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableDropColumn) {
            return convertAlterTableDrop((IgniteSqlAlterTableDropColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlCreateIndex) {
            return convertAddIndex((IgniteSqlCreateIndex) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropIndex) {
            return convertDropIndex((IgniteSqlDropIndex) ddlNode);
        }

        throw new IgniteException("Unsupported operation ["
                + "sqlNodeKind=" + ddlNode.getKind() + "; "
                + "querySql=\"" + ctx.query() + "\"]");
    }

    /**
     * Converts a given CreateTable AST to a CreateTable command.
     *
     * @param createTblNode Root node of the given AST.
     * @param ctx           Planning context.
     */
    private CreateTableCommand convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommand createTblCmd = new CreateTableCommand();

        createTblCmd.schemaName(deriveSchemaName(createTblNode.name(), ctx));
        createTblCmd.tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"));
        createTblCmd.ifTableExists(createTblNode.ifNotExists());

        if (createTblNode.createOptionList() != null) {
            for (SqlNode optNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption opt = (IgniteSqlCreateTableOption) optNode;

                tblOptionProcessors.getOrDefault(opt.key(), UNSUPPORTED_OPTION_PROCESSOR).process(opt, ctx, createTblCmd);
            }
        }

        List<SqlColumnDeclaration> colDeclarations = createTblNode.columnList().getList().stream()
                .filter(SqlColumnDeclaration.class::isInstance)
                .map(SqlColumnDeclaration.class::cast)
                .collect(Collectors.toList());

        IgnitePlanner planner = ctx.planner();

        List<ColumnDefinition> cols = new ArrayList<>(colDeclarations.size());

        for (SqlColumnDeclaration col : colDeclarations) {
            if (!col.name.isSimple()) {
                throw new IgniteException("Unexpected value of columnName ["
                        + "expected a simple identifier, but was " + col.name + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            String name = col.name.getSimple();
            RelDataType relType = planner.convert(col.dataType);

            Object dflt = null;
            if (col.expression != null) {
                dflt = ((SqlLiteral) col.expression).getValue();
            }

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        createTblCmd.columns(cols);

        List<SqlKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(SqlKeyConstraint.class::isInstance)
                .map(SqlKeyConstraint.class::cast)
                .collect(Collectors.toList());

        if (pkConstraints.size() > 1) {
            throw new IgniteException("Unexpected amount of primary key constraints ["
                    + "expected at most one, but was " + pkConstraints.size() + "; "
                    + "querySql=\"" + ctx.query() + "\"]");
        }

        if (!nullOrEmpty(pkConstraints)) {
            Set<String> dedupSet = new HashSet<>();

            List<String> pkCols = pkConstraints.stream()
                    .map(pk -> pk.getOperandList().get(1))
                    .map(SqlNodeList.class::cast)
                    .flatMap(l -> l.getList().stream())
                    .map(SqlIdentifier.class::cast)
                    .map(SqlIdentifier::getSimple)
                    .filter(dedupSet::add)
                    .collect(Collectors.toList());

            createTblCmd.primaryKeyColumns(pkCols);
        }

        return createTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableAddColumn AST to a AlterTableAddCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx          Planning context.
     */
    private AlterTableAddCommand convertAlterTableAdd(IgniteSqlAlterTableAddColumn alterTblNode, PlanningContext ctx) {
        AlterTableAddCommand alterTblCmd = new AlterTableAddCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());
        alterTblCmd.ifColumnNotExists(alterTblNode.ifNotExistsColumn());

        List<ColumnDefinition> cols = new ArrayList<>(alterTblNode.columns().size());

        for (SqlNode colNode : alterTblNode.columns()) {
            assert colNode instanceof SqlColumnDeclaration : colNode.getClass();

            SqlColumnDeclaration col = (SqlColumnDeclaration) colNode;

            assert col.name.isSimple();

            Object dflt = null;
            if (col.expression != null) {
                dflt = ((SqlLiteral) col.expression).getValue();
            }

            String name = col.name.getSimple();
            RelDataType relType = ctx.planner().convert(col.dataType);

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableDropColumn AST to a AlterTableDropCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx          Planning context.
     */
    private AlterTableDropCommand convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropCommand alterTblCmd = new AlterTableDropCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());
        alterTblCmd.ifColumnExists(alterTblNode.ifExistsColumn());

        Set<String> cols = new HashSet<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier) c).getSimple()));

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given DropTable AST to a DropTable command.
     *
     * @param dropTblNode Root node of the given AST.
     * @param ctx         Planning context.
     */
    private DropTableCommand convertDropTable(SqlDropTable dropTblNode, PlanningContext ctx) {
        DropTableCommand dropTblCmd = new DropTableCommand();

        dropTblCmd.schemaName(deriveSchemaName(dropTblNode.name, ctx));
        dropTblCmd.tableName(deriveObjectName(dropTblNode.name, ctx, "tableName"));
        dropTblCmd.ifTableExists(dropTblNode.ifExists);

        return dropTblCmd;
    }

    /**
     * Converts create index to appropriate wrapper.
     */
    private CreateIndexCommand convertAddIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
        CreateIndexCommand createIdxCmd = new CreateIndexCommand();

        createIdxCmd.schemaName(deriveSchemaName(sqlCmd.tableName(), ctx));
        createIdxCmd.tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"));
        createIdxCmd.indexName(sqlCmd.indexName().getSimple());

        List<Pair<String, Boolean>> cols = new ArrayList<>(sqlCmd.columnList().size());

        for (SqlNode col : sqlCmd.columnList().getList()) {
            boolean desc = false;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                desc = true;
            }

            cols.add(new Pair<>(((SqlIdentifier) col).getSimple(), desc));
        }

        createIdxCmd.columns(cols);

        createIdxCmd.ifIndexNotExists(sqlCmd.ifNotExists());

        return createIdxCmd;
    }

    /**
     * Converts drop index to appropriate wrapper.
     */
    private DropIndexCommand convertDropIndex(IgniteSqlDropIndex sqlCmd) {
        DropIndexCommand dropCmd = new DropIndexCommand();

        dropCmd.indexName(sqlCmd.idxName().getSimple());
        dropCmd.ifExist(sqlCmd.ifExists());

        return dropCmd;
    }

    /** Derives a schema name from the compound identifier. */
    private String deriveSchemaName(SqlIdentifier id, PlanningContext ctx) {
        String schemaName;
        if (id.isSimple()) {
            schemaName = ctx.schemaName();
        } else {
            SqlIdentifier schemaId = id.skipLast(1);

            if (!schemaId.isSimple()) {
                throw new IgniteException("Unexpected value of schemaName ["
                        + "expected a simple identifier, but was " + schemaId + "; "
                        + "querySql=\"" + ctx.query() + "\"]"/*, IgniteQueryErrorCode.PARSING*/);
            }

            schemaName = schemaId.getSimple();
        }

        ensureSchemaExists(ctx, schemaName);

        return schemaName;
    }

    /** Derives an object(a table, an index, etc) name from the compound identifier. */
    private String deriveObjectName(SqlIdentifier id, PlanningContext ctx, String objDesc) {
        if (id.isSimple()) {
            return id.getSimple();
        }

        SqlIdentifier objId = id.getComponent(id.skipLast(1).names.size());

        if (!objId.isSimple()) {
            throw new IgniteException("Unexpected value of " + objDesc + " ["
                    + "expected a simple identifier, but was " + objId + "; "
                    + "querySql=\"" + ctx.query() + "\"]"/*, IgniteQueryErrorCode.PARSING*/);
        }

        return objId.getSimple();
    }

    private void ensureSchemaExists(PlanningContext ctx, String schemaName) {
        if (ctx.catalogReader().getRootSchema().getSubSchema(schemaName, true) == null) {
            throw new IgniteException("Schema with name " + schemaName + " not found"/*,
                IgniteQueryErrorCode.SCHEMA_NOT_FOUND*/);
        }
    }

    /**
     * Short cut for validating that option value is a simple identifier.
     *
     * @param opt An option to validate.
     * @param ctx Planning context.
     */
    private String paramIsSqlIdentifierValidator(IgniteSqlCreateTableOption opt, PlanningContext ctx) {
        if (!(opt.value() instanceof SqlIdentifier) || !((SqlIdentifier) opt.value()).isSimple()) {
            throwOptionParsingException(opt, "a simple identifier", ctx.query());
        }

        return ((SqlIdentifier) opt.value()).getSimple();
    }

    /**
     * Creates a validator for an option which value should be value of given enumeration.
     *
     * @param clz Enumeration class to create validator for.
     */
    private static <T extends Enum<T>> BiFunction<IgniteSqlCreateTableOption, PlanningContext, T> validatorForEnumValue(
            Class<T> clz
    ) {
        return (opt, ctx) -> {
            T val = null;

            if (opt.value() instanceof SqlIdentifier) {
                val = Arrays.stream(clz.getEnumConstants())
                        .filter(m -> m.name().equalsIgnoreCase(opt.value().toString()))
                        .findFirst()
                        .orElse(null);
            }

            if (val == null) {
                throwOptionParsingException(opt, "values are "
                        + Arrays.toString(clz.getEnumConstants()), ctx.query());
            }

            return val;
        };
    }

    /**
     * Throws exception with message relates to validation of create table option.
     *
     * @param opt An option which validation was failed.
     * @param exp A string representing expected values.
     * @param qry A query the validation was failed for.
     */
    private static void throwOptionParsingException(IgniteSqlCreateTableOption opt, String exp, String qry) {
        throw new IgniteException("Unexpected value for param " + opt.key() + " ["
                + "expected " + exp + ", but was " + opt.value() + "; "
                + "querySql=\"" + qry + "\"]"/*, IgniteQueryErrorCode.PARSING*/);
    }

    private static class TableOptionProcessor<T> {
        private final IgniteSqlCreateTableOptionEnum key;

        private final BiFunction<IgniteSqlCreateTableOption, PlanningContext, T> validator;

        private final BiConsumer<CreateTableCommand, T> valSetter;

        /**
         * Constructor.
         *
         * @param key       Option key this processor is supopsed to handle.
         * @param validator Validator that derives a value from a {@link SqlNode}, validates it and then returns if validation passed,
         *                  throws an exeption otherwise.
         * @param valSetter Setter sets the value recived from the validator to the given {@link CreateTableCommand}.
         */
        private TableOptionProcessor(
                IgniteSqlCreateTableOptionEnum key,
                BiFunction<IgniteSqlCreateTableOption, PlanningContext, T> validator,
                BiConsumer<CreateTableCommand, T> valSetter
        ) {
            this.key = key;
            this.validator = validator;
            this.valSetter = valSetter;
        }

        /**
         * Processes the given option, validates it's value and then sets the appropriate field in a given command, throws an exception if
         * the validation failed.
         *
         * @param opt Option to validate.
         * @param ctx Planning context.
         * @param cmd Command instance to set a validation result.
         */
        private void process(IgniteSqlCreateTableOption opt, PlanningContext ctx, CreateTableCommand cmd) {
            assert key == null || key == opt.key() : "Unexpected create table option [expected=" + key + ", actual="
                    + opt.key() + "]";

            valSetter.accept(cmd, validator.apply(opt, ctx));
        }

        /**
         * Get key this processor is supposed to handle.
         */
        private IgniteSqlCreateTableOptionEnum key() {
            return key;
        }
    }
}
