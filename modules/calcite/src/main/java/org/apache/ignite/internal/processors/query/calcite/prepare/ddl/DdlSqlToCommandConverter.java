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

import java.lang.reflect.Type;
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
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseDataContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCommit;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOption;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlRollback;
import org.apache.ignite.internal.processors.query.calcite.type.OtherType;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.AFFINITY_KEY;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.ATOMICITY;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.BACKUPS;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.CACHE_GROUP;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.CACHE_NAME;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.DATA_REGION;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.ENCRYPTED;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.KEY_TYPE;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.TEMPLATE;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.VALUE_TYPE;
import static org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTableOptionEnum.WRITE_SYNCHRONIZATION_MODE;
import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveObjectName;
import static org.apache.ignite.internal.processors.query.calcite.util.PlanUtils.deriveSchemaName;

/** */
public class DdlSqlToCommandConverter {
    /** Processor that validates a value is a Sql Identifier. */
    private static final BiFunction<IgniteSqlCreateTableOption, PlanningContext, String> VALUE_IS_IDENTIFIER_VALIDATOR = (opt, ctx) -> {
        if (!(opt.value() instanceof SqlIdentifier) || !((SqlIdentifier)opt.value()).isSimple())
            throwOptionParsingException(opt, "a simple identifier", ctx.query());

        return ((SqlIdentifier)opt.value()).getSimple();
    };

    /** Processor that unconditionally throws an AssertionException. */
    private static final TableOptionProcessor<Void> UNSUPPORTED_OPTION_PROCESSOR = new TableOptionProcessor<>(
        null,
        (opt, ctx) -> {
            throw new AssertionError("Unsupported option " + opt.key());
        },
        null);

    /** Map of the supported table option processors. */
    private final Map<IgniteSqlCreateTableOptionEnum, TableOptionProcessor<?>> tblOptionProcessors = Stream.of(
        new TableOptionProcessor<>(TEMPLATE, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::templateName),
        new TableOptionProcessor<>(AFFINITY_KEY, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::affinityKey),
        new TableOptionProcessor<>(CACHE_GROUP, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::cacheGroup),
        new TableOptionProcessor<>(CACHE_NAME, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::cacheName),
        new TableOptionProcessor<>(DATA_REGION, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::dataRegionName),
        new TableOptionProcessor<>(KEY_TYPE, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::keyTypeName),
        new TableOptionProcessor<>(VALUE_TYPE, VALUE_IS_IDENTIFIER_VALIDATOR, CreateTableCommand::valueTypeName),
        new TableOptionProcessor<>(ATOMICITY, validatorForEnumValue(CacheAtomicityMode.class), CreateTableCommand::atomicityMode),
        new TableOptionProcessor<>(WRITE_SYNCHRONIZATION_MODE, validatorForEnumValue(CacheWriteSynchronizationMode.class),
            CreateTableCommand::writeSynchronizationMode),
        new TableOptionProcessor<>(BACKUPS, (opt, ctx) -> {
            if (opt.value() instanceof SqlIdentifier) {
                String val = VALUE_IS_IDENTIFIER_VALIDATOR.apply(opt, ctx);

                try {
                    int intVal = Integer.parseInt(val);

                    if (intVal < 0)
                        throwOptionParsingException(opt, "a non-negative integer", ctx.query());

                    return intVal;
                }
                catch (NumberFormatException e) {
                    throwOptionParsingException(opt, "a non-negative integer", ctx.query());
                }
            }

            if (!(opt.value() instanceof SqlNumericLiteral)
                || !((SqlNumericLiteral)opt.value()).isInteger()
                || ((SqlLiteral)opt.value()).intValue(true) < 0
            )
                throwOptionParsingException(opt, "a non-negative integer", ctx.query());

            return ((SqlLiteral)opt.value()).intValue(true);
        }, CreateTableCommand::backups),
        new TableOptionProcessor<>(ENCRYPTED, (opt, ctx) -> {
            if (opt.value() instanceof SqlIdentifier) {
                String val = VALUE_IS_IDENTIFIER_VALIDATOR.apply(opt, ctx);

                return Boolean.parseBoolean(val);
            }

            if (!(opt.value() instanceof SqlLiteral) && ((SqlLiteral)opt.value()).getTypeName() != BOOLEAN)
                throwOptionParsingException(opt, "a boolean", ctx.query());

            return ((SqlLiteral)opt.value()).booleanValue();
        }, CreateTableCommand::encrypted)
        ).collect(Collectors.toMap(TableOptionProcessor::key, Function.identity()));

    /**
     * Converts a given ddl AST to a ddl command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx Planning context.
     */
    public DdlCommand convert(SqlDdl ddlNode, PlanningContext ctx) {
        if (ddlNode instanceof IgniteSqlCreateTable)
            return convertCreateTable((IgniteSqlCreateTable)ddlNode, ctx);

        if (ddlNode instanceof SqlDropTable)
            return convertDropTable((SqlDropTable)ddlNode, ctx);

        if (ddlNode instanceof IgniteSqlAlterTableAddColumn)
            return convertAlterTableAdd((IgniteSqlAlterTableAddColumn)ddlNode, ctx);

        if (ddlNode instanceof IgniteSqlAlterTableDropColumn)
            return convertAlterTableDrop((IgniteSqlAlterTableDropColumn)ddlNode, ctx);

        if (ddlNode instanceof IgniteSqlCommit || ddlNode instanceof IgniteSqlRollback)
            return new TransactionCommand();

        if (SqlToNativeCommandConverter.isSupported(ddlNode))
            return SqlToNativeCommandConverter.convert(ddlNode, ctx);

        throw new IgniteSQLException("Unsupported operation [" +
            "sqlNodeKind=" + ddlNode.getKind() + "; " +
            "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Converts a given CreateTable AST to a CreateTable command.
     *
     * @param createTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private CreateTableCommand convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommand createTblCmd = new CreateTableCommand();

        String schemaName = deriveSchemaName(createTblNode.name(), ctx);
        String tableName = deriveObjectName(createTblNode.name(), ctx, "tableName");

        createTblCmd.schemaName(schemaName);
        createTblCmd.tableName(tableName);
        createTblCmd.ifNotExists(createTblNode.ifNotExists());
        createTblCmd.templateName(QueryUtils.TEMPLATE_PARTITIONED);

        if (createTblNode.createOptionList() != null) {
            for (SqlNode optNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption opt = (IgniteSqlCreateTableOption)optNode;

                tblOptionProcessors.getOrDefault(opt.key(), UNSUPPORTED_OPTION_PROCESSOR).process(opt, ctx, createTblCmd);
            }
        }

        IgnitePlanner planner = ctx.planner();

        if (createTblNode.query() == null) {
            List<SqlColumnDeclaration> colDeclarations = createTblNode.columnList().getList().stream()
                .filter(SqlColumnDeclaration.class::isInstance)
                .map(SqlColumnDeclaration.class::cast)
                .collect(Collectors.toList());

            List<ColumnDefinition> cols = new ArrayList<>();

            for (SqlColumnDeclaration col : colDeclarations) {
                if (!col.name.isSimple())
                    throw new IgniteSQLException("Unexpected value of columnName [" +
                        "expected a simple identifier, but was " + col.name + "; " +
                        "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.PARSING);

                String name = col.name.getSimple();
                RelDataType type = planner.convert(col.dataType);

                Object dflt = null;
                if (col.expression != null) {
                    assert col.expression instanceof SqlLiteral;

                    Type storageType = ctx.typeFactory().getResultClass(type);

                    DataContext dataCtx = new BaseDataContext(ctx.typeFactory());

                    if (type instanceof OtherType)
                        throw new IgniteSQLException("Type '" + type + "' doesn't support default value.");

                    dflt = TypeUtils.fromLiteral(dataCtx, storageType, (SqlLiteral)col.expression);
                }

                cols.add(new ColumnDefinition(name, type, dflt));
            }

            createTblCmd.columns(cols);

            List<SqlKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(SqlKeyConstraint.class::isInstance)
                .map(SqlKeyConstraint.class::cast)
                .collect(Collectors.toList());

            if (pkConstraints.size() > 1)
                throw new IgniteSQLException("Unexpected amount of primary key constraints [" +
                    "expected at most one, but was " + pkConstraints.size() + "; " +
                    "querySql=\"" + ctx.query() + "\"]", IgniteQueryErrorCode.PARSING);

            if (!F.isEmpty(pkConstraints)) {
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
        }
        else { // CREATE AS SELECT.
            ValidationResult res = planner.validateAndGetTypeMetadata(createTblNode.query());

            // Create INSERT node on top of AS SELECT node.
            SqlInsert sqlInsert = new SqlInsert(
                createTblNode.query().getParserPosition(),
                SqlNodeList.EMPTY,
                createTblNode.name(),
                res.sqlNode(),
                null
            );

            createTblCmd.insertStatement(sqlInsert);

            List<RelDataTypeField> fields = res.dataType().getFieldList();
            List<ColumnDefinition> cols = new ArrayList<>(fields.size());

            if (createTblNode.columnList() != null) {
                // Derive column names from the CREATE TABLE clause and column types from the query.
                List<SqlIdentifier> colNames = createTblNode.columnList().getList().stream()
                    .map(SqlIdentifier.class::cast)
                    .collect(Collectors.toList());

                if (fields.size() != colNames.size()) {
                    throw new IgniteSQLException("Number of columns must match number of query columns",
                        IgniteQueryErrorCode.PARSING);
                }

                for (int i = 0; i < colNames.size(); i++) {
                    SqlIdentifier colName = colNames.get(i);

                    assert colName.isSimple();

                    RelDataType type = fields.get(i).getType();

                    cols.add(new ColumnDefinition(colName.getSimple(), type, null));
                }
            }
            else {
                // Derive column names and column types from the query.
                for (RelDataTypeField field : fields)
                    cols.add(new ColumnDefinition(field.getName(), field.getType(), null));
            }

            createTblCmd.columns(cols);
        }

        if (createTblCmd.columns() == null) {
            throw new IgniteSQLException("Column list or query should be specified for CREATE TABLE command",
                IgniteQueryErrorCode.PARSING);
        }

        return createTblCmd;
    }

    /**
     * Converts a given DropTable AST to a DropTable command.
     *
     * @param dropTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private DropTableCommand convertDropTable(SqlDropTable dropTblNode, PlanningContext ctx) {
        DropTableCommand dropTblCmd = new DropTableCommand();

        dropTblCmd.schemaName(deriveSchemaName(dropTblNode.name, ctx));
        dropTblCmd.tableName(deriveObjectName(dropTblNode.name, ctx, "tableName"));
        dropTblCmd.ifExists(dropTblNode.ifExists);

        return dropTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableAddColumn AST to a AlterTableAddCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx Planning context.
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

            SqlColumnDeclaration col = (SqlColumnDeclaration)colNode;

            assert col.name.isSimple();

            String name = col.name.getSimple();
            RelDataType type = ctx.planner().convert(col.dataType);

            assert col.expression == null : "Unexpected column default value" + col.expression;

            cols.add(new ColumnDefinition(name, type, null));
        }

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableDropColumn AST to a AlterTableDropCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private AlterTableDropCommand convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropCommand alterTblCmd = new AlterTableDropCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());
        alterTblCmd.ifColumnExists(alterTblNode.ifExistsColumn());

        List<String> cols = new ArrayList<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier)c).getSimple()));

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Short cut for validating that option value is a simple identifier.
     *
     * @param opt An option to validate.
     * @param ctx Planning context.
     * @throws IgniteSQLException In case the validation was failed.
     */
    private String paramIsSqlIdentifierValidator(IgniteSqlCreateTableOption opt, PlanningContext ctx) {
        if (!(opt.value() instanceof SqlIdentifier) || !((SqlIdentifier)opt.value()).isSimple())
            throwOptionParsingException(opt, "a simple identifier", ctx.query());

        return ((SqlIdentifier)opt.value()).getSimple();
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

            if (val == null)
                throwOptionParsingException(opt, "values are "
                    + Arrays.toString(clz.getEnumConstants()), ctx.query());

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
        throw new IgniteSQLException("Unexpected value for param " + opt.key() + " [" +
            "expected " + exp + ", but was " + opt.value() + "; " +
            "querySql=\"" + qry + "\"]", IgniteQueryErrorCode.PARSING);
    }

    /** */
    private static class TableOptionProcessor<T> {
        /** */
        private final IgniteSqlCreateTableOptionEnum key;

        /** */
        private final BiFunction<IgniteSqlCreateTableOption, PlanningContext, T> validator;

        /** */
        private final BiConsumer<CreateTableCommand, T> valSetter;

        /**
         * @param key Option key this processor is supopsed to handle.
         * @param validator Validator that derives a value from a {@link SqlNode},
         *                 validates it and then returns if validation passed,
         *                 throws an exeption otherwise.
         * @param valSetter Setter sets the value recived from the validator
         *                 to the given {@link CreateTableCommand}.
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
         * Processes the given option, validates it's value and then sets the appropriate
         * field in a given command, throws an exception if the validation failed.
         *
         * @param opt Option to validate.
         * @param ctx Planning context.
         * @param cmd Command instance to set a validation result.
         */
        private void process(IgniteSqlCreateTableOption opt, PlanningContext ctx, CreateTableCommand cmd) {
            assert key == null || key == opt.key() : "Unexpected create table option [expected=" + key + ", actual=" + opt.key() + "]";

            valSetter.accept(cmd, validator.apply(opt, ctx));
        }

        /**
         * @return Key this processor is supposed to handle.
         */
        private IgniteSqlCreateTableOptionEnum key() {
            return key;
        }
    }
}
