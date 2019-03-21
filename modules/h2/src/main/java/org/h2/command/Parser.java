/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 * Support for the operator "&&" as an alias for SPATIAL_INTERSECTS
 */
package org.h2.command;

import static org.h2.util.ParserUtil.ALL;
import static org.h2.util.ParserUtil.ARRAY;
import static org.h2.util.ParserUtil.CASE;
import static org.h2.util.ParserUtil.CHECK;
import static org.h2.util.ParserUtil.CONSTRAINT;
import static org.h2.util.ParserUtil.CROSS;
import static org.h2.util.ParserUtil.CURRENT_DATE;
import static org.h2.util.ParserUtil.CURRENT_TIME;
import static org.h2.util.ParserUtil.CURRENT_TIMESTAMP;
import static org.h2.util.ParserUtil.CURRENT_USER;
import static org.h2.util.ParserUtil.DISTINCT;
import static org.h2.util.ParserUtil.EXCEPT;
import static org.h2.util.ParserUtil.EXISTS;
import static org.h2.util.ParserUtil.FALSE;
import static org.h2.util.ParserUtil.FETCH;
import static org.h2.util.ParserUtil.FOR;
import static org.h2.util.ParserUtil.FOREIGN;
import static org.h2.util.ParserUtil.FROM;
import static org.h2.util.ParserUtil.FULL;
import static org.h2.util.ParserUtil.GROUP;
import static org.h2.util.ParserUtil.HAVING;
import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.IF;
import static org.h2.util.ParserUtil.INNER;
import static org.h2.util.ParserUtil.INTERSECT;
import static org.h2.util.ParserUtil.INTERSECTS;
import static org.h2.util.ParserUtil.INTERVAL;
import static org.h2.util.ParserUtil.IS;
import static org.h2.util.ParserUtil.JOIN;
import static org.h2.util.ParserUtil.LIKE;
import static org.h2.util.ParserUtil.LIMIT;
import static org.h2.util.ParserUtil.LOCALTIME;
import static org.h2.util.ParserUtil.LOCALTIMESTAMP;
import static org.h2.util.ParserUtil.MINUS;
import static org.h2.util.ParserUtil.NATURAL;
import static org.h2.util.ParserUtil.NOT;
import static org.h2.util.ParserUtil.NULL;
import static org.h2.util.ParserUtil.OFFSET;
import static org.h2.util.ParserUtil.ON;
import static org.h2.util.ParserUtil.ORDER;
import static org.h2.util.ParserUtil.PRIMARY;
import static org.h2.util.ParserUtil.QUALIFY;
import static org.h2.util.ParserUtil.ROW;
import static org.h2.util.ParserUtil.ROWNUM;
import static org.h2.util.ParserUtil.SELECT;
import static org.h2.util.ParserUtil.TABLE;
import static org.h2.util.ParserUtil.TRUE;
import static org.h2.util.ParserUtil.UNION;
import static org.h2.util.ParserUtil.UNIQUE;
import static org.h2.util.ParserUtil.VALUES;
import static org.h2.util.ParserUtil.WHERE;
import static org.h2.util.ParserUtil.WINDOW;
import static org.h2.util.ParserUtil.WITH;
import static org.h2.util.ParserUtil._ROWID_;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.api.Trigger;
import org.h2.command.ddl.AlterIndexRename;
import org.h2.command.ddl.AlterSchemaRename;
import org.h2.command.ddl.AlterSequence;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.AlterTableDropConstraint;
import org.h2.command.ddl.AlterTableRename;
import org.h2.command.ddl.AlterTableRenameColumn;
import org.h2.command.ddl.AlterTableRenameConstraint;
import org.h2.command.ddl.AlterUser;
import org.h2.command.ddl.AlterView;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CommandWithColumns;
import org.h2.command.ddl.CreateAggregate;
import org.h2.command.ddl.CreateConstant;
import org.h2.command.ddl.CreateDomain;
import org.h2.command.ddl.CreateFunctionAlias;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateLinkedTable;
import org.h2.command.ddl.CreateRole;
import org.h2.command.ddl.CreateSchema;
import org.h2.command.ddl.CreateSequence;
import org.h2.command.ddl.CreateSynonym;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.CreateTrigger;
import org.h2.command.ddl.CreateUser;
import org.h2.command.ddl.CreateView;
import org.h2.command.ddl.DeallocateProcedure;
import org.h2.command.ddl.DefineCommand;
import org.h2.command.ddl.DropAggregate;
import org.h2.command.ddl.DropConstant;
import org.h2.command.ddl.DropDatabase;
import org.h2.command.ddl.DropDomain;
import org.h2.command.ddl.DropFunctionAlias;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropRole;
import org.h2.command.ddl.DropSchema;
import org.h2.command.ddl.DropSequence;
import org.h2.command.ddl.DropSynonym;
import org.h2.command.ddl.DropTable;
import org.h2.command.ddl.DropTrigger;
import org.h2.command.ddl.DropUser;
import org.h2.command.ddl.DropView;
import org.h2.command.ddl.GrantRevoke;
import org.h2.command.ddl.PrepareProcedure;
import org.h2.command.ddl.SchemaCommand;
import org.h2.command.ddl.SequenceOptions;
import org.h2.command.ddl.SetComment;
import org.h2.command.ddl.TruncateTable;
import org.h2.command.dml.AlterTableSet;
import org.h2.command.dml.BackupCommand;
import org.h2.command.dml.Call;
import org.h2.command.dml.CommandWithValues;
import org.h2.command.dml.Delete;
import org.h2.command.dml.ExecuteProcedure;
import org.h2.command.dml.Explain;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.MergeUsing;
import org.h2.command.dml.NoOperation;
import org.h2.command.dml.Query;
import org.h2.command.dml.Replace;
import org.h2.command.dml.RunScriptCommand;
import org.h2.command.dml.ScriptCommand;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.command.dml.SelectUnion;
import org.h2.command.dml.Set;
import org.h2.command.dml.SetTypes;
import org.h2.command.dml.TransactionCommand;
import org.h2.command.dml.Update;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Domain;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Mode;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.Procedure;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.expression.Alias;
import org.h2.expression.BinaryOperation;
import org.h2.expression.BinaryOperation.OpType;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.Parameter;
import org.h2.expression.Rownum;
import org.h2.expression.SequenceValue;
import org.h2.expression.Subquery;
import org.h2.expression.UnaryOperation;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.expression.Wildcard;
import org.h2.expression.aggregate.AbstractAggregate;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.JavaAggregate;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.Window;
import org.h2.expression.analysis.WindowFrame;
import org.h2.expression.analysis.WindowFrameBound;
import org.h2.expression.analysis.WindowFrameBoundType;
import org.h2.expression.analysis.WindowFrameExclusion;
import org.h2.expression.analysis.WindowFrameUnits;
import org.h2.expression.analysis.WindowFunction;
import org.h2.expression.analysis.WindowFunctionType;
import org.h2.expression.condition.CompareLike;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionExists;
import org.h2.expression.condition.ConditionIn;
import org.h2.expression.condition.ConditionInParameter;
import org.h2.expression.condition.ConditionInSelect;
import org.h2.expression.condition.ConditionNot;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionCall;
import org.h2.expression.function.JavaFunction;
import org.h2.expression.function.TableFunction;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.IndexHints;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableFilter.TableFilterVisitor;
import org.h2.table.TableView;
import org.h2.util.IntervalUtils;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.ExtTypeInfo;
import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.ExtTypeInfoGeometry;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueInt;
import org.h2.value.ValueInterval;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * The parser is used to convert a SQL statement string to an command object.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class Parser {

    private static final String WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS =
            "WITH statement supports only SELECT, TABLE, VALUES, " +
            "CREATE TABLE, INSERT, UPDATE, MERGE or DELETE statements";

    // used during the tokenizer phase
    private static final int CHAR_END = 1, CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5,
            CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DOT = 8,
            CHAR_DOLLAR_QUOTED_STRING = 9;

    // this are token types, see also types in ParserUtil

    /**
     * Token with parameter.
     */
    private static final int PARAMETER = WITH + 1;

    /**
     * End of input.
     */
    private static final int END = PARAMETER + 1;

    /**
     * Token with value.
     */
    private static final int VALUE = END + 1;

    /**
     * The token "=".
     */
    private static final int EQUAL = VALUE + 1;

    /**
     * The token ">=".
     */
    private static final int BIGGER_EQUAL = EQUAL + 1;

    /**
     * The token ">".
     */
    private static final int BIGGER = BIGGER_EQUAL + 1;

    /**
     * The token "<".
     */
    private static final int SMALLER = BIGGER + 1;

    /**
     * The token "<=".
     */
    private static final int SMALLER_EQUAL = SMALLER + 1;

    /**
     * The token "<>" or "!=".
     */
    private static final int NOT_EQUAL = SMALLER_EQUAL + 1;

    /**
     * The token "@".
     */
    private static final int AT = NOT_EQUAL + 1;

    /**
     * The token "-".
     */
    private static final int MINUS_SIGN = AT + 1;

    /**
     * The token "+".
     */
    private static final int PLUS_SIGN = MINUS_SIGN + 1;

    /**
     * The token "||".
     */
    private static final int STRING_CONCAT = PLUS_SIGN + 1;

    /**
     * The token "(".
     */
    private static final int OPEN_PAREN = STRING_CONCAT + 1;

    /**
     * The token ")".
     */
    private static final int CLOSE_PAREN = OPEN_PAREN + 1;

    /**
     * The token "&amp;&amp;".
     */
    private static final int SPATIAL_INTERSECTS = CLOSE_PAREN + 1;

    /**
     * The token "*".
     */
    private static final int ASTERISK = SPATIAL_INTERSECTS + 1;

    /**
     * The token ",".
     */
    private static final int COMMA = ASTERISK + 1;

    /**
     * The token ".".
     */
    private static final int DOT = COMMA + 1;

    /**
     * The token "{".
     */
    private static final int OPEN_BRACE = DOT + 1;

    /**
     * The token "}".
     */
    private static final int CLOSE_BRACE = OPEN_BRACE + 1;

    /**
     * The token "/".
     */
    private static final int SLASH = CLOSE_BRACE + 1;

    /**
     * The token "%".
     */
    private static final int PERCENT = SLASH + 1;

    /**
     * The token ";".
     */
    private static final int SEMICOLON = PERCENT + 1;

    /**
     * The token ":".
     */
    private static final int COLON = SEMICOLON + 1;

    /**
     * The token "[".
     */
    private static final int OPEN_BRACKET = COLON + 1;

    /**
     * The token "]".
     */
    private static final int CLOSE_BRACKET = OPEN_BRACKET + 1;

    /**
     * The token "~".
     */
    private static final int TILDE = CLOSE_BRACKET + 1;

    /**
     * The token "::".
     */
    private static final int COLON_COLON = TILDE + 1;

    /**
     * The token ":=".
     */
    private static final int COLON_EQ = COLON_COLON + 1;

    /**
     * The token "!~".
     */
    private static final int NOT_TILDE = COLON_EQ + 1;

    private static final String[] TOKENS = {
            // Unused
            null,
            // KEYWORD
            null,
            // IDENTIFIER
            null,
            // ALL
            "ALL",
            // ARRAY
            "ARRAY",
            // CASE
            "CASE",
            // CHECK
            "CHECK",
            // CONSTRAINT
            "CONSTRAINT",
            // CROSS
            "CROSS",
            // CURRENT_DATE
            "CURRENT_DATE",
            // CURRENT_TIME
            "CURRENT_TIME",
            // CURRENT_TIMESTAMP
            "CURRENT_TIMESTAMP",
            // CURRENT_USER
            "CURRENT_USER",
            // DISTINCT
            "DISTINCT",
            // EXCEPT
            "EXCEPT",
            // EXISTS
            "EXISTS",
            // FALSE
            "FALSE",
            // FETCH
            "FETCH",
            // FOR
            "FOR",
            // FOREIGN
            "FOREIGN",
            // FROM
            "FROM",
            // FULL
            "FULL",
            // GROUP
            "GROUP",
            // HAVING
            "HAVING",
            // IF
            "IF",
            // INNER
            "INNER",
            // INTERSECT
            "INTERSECT",
            // INTERSECTS
            "INTERSECTS",
            // INTERVAL
            "INTERVAL",
            // IS
            "IS",
            // JOIN
            "JOIN",
            // LIKE
            "LIKE",
            // LIMIT
            "LIMIT",
            // LOCALTIME
            "LOCALTIME",
            // LOCALTIMESTAMP
            "LOCALTIMESTAMP",
            // MINUS
            "MINUS",
            // NATURAL
            "NATURAL",
            // NOT
            "NOT",
            // NULL
            "NULL",
            // OFFSET
            "OFFSET",
            // ON
            "ON",
            // ORDER
            "ORDER",
            // PRIMARY
            "PRIMARY",
            // QUALIFY
            "QUALIFY",
            // ROW
            "ROW",
            // _ROWID_
            "_ROWID_",
            // ROWNUM
            "ROWNUM",
            // SELECT
            "SELECT",
            // TABLE
            "TABLE",
            // TRUE
            "TRUE",
            // UNION
            "UNION",
            // UNIQUE
            "UNIQUE",
            // VALUES
            "VALUES",
            // WHERE
            "WHERE",
            // WINDOW
            "WINDOW",
            // WITH
            "WITH",
            // PARAMETER
            "?",
            // END
            null,
            // VALUE
            null,
            // EQUAL
            "=",
            // BIGGER_EQUAL
            ">=",
            // BIGGER
            ">",
            // SMALLER
            "<",
            // SMALLER_EQUAL
            "<=",
            // NOT_EQUAL
            "<>",
            // AT
            "@",
            // MINUS_SIGN
            "-",
            // PLUS_SIGN
            "+",
            // STRING_CONCAT
            "||",
            // OPEN_PAREN
            "(",
            // CLOSE_PAREN
            ")",
            // SPATIAL_INTERSECTS
            "&&",
            // ASTERISK
            "*",
            // COMMA
            ",",
            // DOT
            ".",
            // OPEN_BRACE
            "{",
            // CLOSE_BRACE
            "}",
            // SLASH
            "/",
            // PERCENT
            "%",
            // SEMICOLON
            ";",
            // COLON
            ":",
            // OPEN_BRACKET
            "[",
            // CLOSE_BRACKET
            "]",
            // TILDE
            "~",
            // COLON_COLON
            "::",
            // COLON_EQ
            ":=",
            // NOT_TILDE
            "!~",
            // End
    };

    private static final Comparator<TableFilter> TABLE_FILTER_COMPARATOR =
            new Comparator<TableFilter>() {
        @Override
        public int compare(TableFilter o1, TableFilter o2) {
            if (o1 == o2)
                return 0;
            assert o1.getOrderInFrom() != o2.getOrderInFrom();
            return o1.getOrderInFrom() > o2.getOrderInFrom() ? 1 : -1;
        }
    };

    private final Database database;
    private final Session session;

    /**
     * @see org.h2.engine.DbSettings#databaseToLower
     */
    private final boolean identifiersToLower;
    /**
     * @see org.h2.engine.DbSettings#databaseToUpper
     */
    private final boolean identifiersToUpper;

    /** indicates character-type for each char in sqlCommand */
    private int[] characterTypes;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private Value currentValue;
    private String originalSQL;
    /** copy of originalSQL, with comments blanked out */
    private String sqlCommand;
    /** cached array if chars from sqlCommand */
    private char[] sqlCommandChars;
    /** index into sqlCommand of previous token */
    private int lastParseIndex;
    /** index into sqlCommand of current token */
    private int parseIndex;
    private CreateView createView;
    private Prepared currentPrepared;
    private Select currentSelect;
    private ArrayList<Parameter> parameters;
    private ArrayList<Parameter> indexedParameterList;
    private ArrayList<Parameter> suppliedParameters;
    private ArrayList<Parameter> suppliedParameterList;
    private String schemaName;
    private ArrayList<String> expectedList;
    private boolean rightsChecked;
    private boolean recompileAlways;
    private boolean literalsChecked;
    private int orderInFrom;

    public Parser(Session session) {
        this.database = session.getDatabase();
        this.identifiersToLower = database.getSettings().databaseToLower;
        this.identifiersToUpper = database.getSettings().databaseToUpper;
        this.session = session;
    }

    /**
     * Parse the statement and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    public Prepared prepare(String sql) {
        Prepared p = parse(sql);
        p.prepare();
        if (currentTokenType != END) {
            throw getSyntaxError();
        }
        return p;
    }

    /**
     * Parse a statement or a list of statements, and prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the command object
     */
    public Command prepareCommand(String sql) {
        try {
            Prepared p = parse(sql);
            if (currentTokenType != SEMICOLON && currentTokenType != END) {
                addExpected(SEMICOLON);
                throw getSyntaxError();
            }
            try {
                p.prepare();
            } catch (Throwable t) {
                CommandContainer.clearCTE(session, p);
                throw t;
            }
            if (parseIndex < sql.length()) {
                sql = sql.substring(0, parseIndex);
            }
            CommandContainer c = new CommandContainer(session, sql, p);
            if (currentTokenType == SEMICOLON) {
                String remaining = originalSQL.substring(parseIndex);
                if (!StringUtils.isWhitespaceOrEmpty(remaining)) {
                    return prepareCommandList(c, sql, remaining);
                }
            }
            return c;
        } catch (DbException e) {
            throw e.addSQL(originalSQL);
        }
    }

    private CommandList prepareCommandList(CommandContainer command, String sql, String remaining) {
        try {
            ArrayList<Prepared> list = Utils.newSmallArrayList();
            boolean stop = false;
            do {
                if (stop) {
                    return new CommandList(session, sql, command, list, parameters, remaining);
                }
                suppliedParameters = parameters;
                suppliedParameterList = indexedParameterList;
                Prepared p;
                try {
                    p = parse(remaining);
                } catch (DbException ex) {
                    // This command may depend on results of previous commands.
                    if (ex.getErrorCode() == ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS) {
                        throw ex;
                    }
                    return new CommandList(session, sql, command, list, parameters, remaining);
                }
                if (p instanceof DefineCommand) {
                    // Next commands may depend on results of this command.
                    stop = true;
                }
                list.add(p);
                if (currentTokenType == END) {
                    break;
                }
                if (currentTokenType != SEMICOLON) {
                    addExpected(SEMICOLON);
                    throw getSyntaxError();
                }
            } while (!StringUtils.isWhitespaceOrEmpty(remaining = originalSQL.substring(parseIndex)));
            return new CommandList(session, sql, command, list, parameters, null);
        } catch (Throwable t) {
            command.clearCTE();
            throw t;
        }
    }

    /**
     * Parse the statement, but don't prepare it for execution.
     *
     * @param sql the SQL statement to parse
     * @return the prepared object
     */
    Prepared parse(String sql) {
        Prepared p;
        try {
            // first, try the fast variant
            p = parse(sql, false);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.SYNTAX_ERROR_1) {
                // now, get the detailed exception
                p = parse(sql, true);
            } else {
                throw e.addSQL(sql);
            }
        }
        p.setPrepareAlways(recompileAlways);
        p.setParameterList(parameters);
        return p;
    }

    private Prepared parse(String sql, boolean withExpectedList) {
        initialize(sql);
        if (withExpectedList) {
            expectedList = new ArrayList<>();
        } else {
            expectedList = null;
        }
        parameters = suppliedParameters != null ? suppliedParameters : Utils.<Parameter>newSmallArrayList();
        indexedParameterList = suppliedParameterList;
        currentSelect = null;
        currentPrepared = null;
        createView = null;
        recompileAlways = false;
        read();
        return parsePrepared();
    }

    private Prepared parsePrepared() {
        int start = lastParseIndex;
        Prepared c = null;
        switch (currentTokenType) {
        case END:
        case SEMICOLON:
            c = new NoOperation(session);
            setSQL(c, null, start);
            return c;
        case PARAMETER:
            // read the ? as a parameter
            readTerm();
            // this is an 'out' parameter - set a dummy value
            parameters.get(0).setValue(ValueNull.INSTANCE);
            read(EQUAL);
            read("CALL");
            c = parseCall();
            break;
        case OPEN_PAREN:
        case FROM:
        case SELECT:
        case TABLE:
        case VALUES:
            c = parseSelect();
            break;
        case WITH:
            read();
            c = parseWithStatementOrQuery();
            break;
        case IDENTIFIER:
            if (currentTokenQuoted) {
                break;
            }
            switch (currentToken.charAt(0)) {
            case 'a':
            case 'A':
                if (readIf("ALTER")) {
                    c = parseAlter();
                } else if (readIf("ANALYZE")) {
                    c = parseAnalyze();
                }
                break;
            case 'b':
            case 'B':
                if (readIf("BACKUP")) {
                    c = parseBackup();
                } else if (readIf("BEGIN")) {
                    c = parseBegin();
                }
                break;
            case 'c':
            case 'C':
                if (readIf("COMMIT")) {
                    c = parseCommit();
                } else if (readIf("CREATE")) {
                    c = parseCreate();
                } else if (readIf("CALL")) {
                    c = parseCall();
                } else if (readIf("CHECKPOINT")) {
                    c = parseCheckpoint();
                } else if (readIf("COMMENT")) {
                    c = parseComment();
                }
                break;
            case 'd':
            case 'D':
                if (readIf("DELETE")) {
                    c = parseDelete();
                } else if (readIf("DROP")) {
                    c = parseDrop();
                } else if (readIf("DECLARE")) {
                    // support for DECLARE GLOBAL TEMPORARY TABLE...
                    c = parseCreate();
                } else if (readIf("DEALLOCATE")) {
                    c = parseDeallocate();
                }
                break;
            case 'e':
            case 'E':
                if (readIf("EXPLAIN")) {
                    c = parseExplain();
                } else if (readIf("EXECUTE")) {
                    c = parseExecute();
                }
                break;
            case 'g':
            case 'G':
                if (readIf("GRANT")) {
                    c = parseGrantRevoke(CommandInterface.GRANT);
                }
                break;
            case 'h':
            case 'H':
                if (readIf("HELP")) {
                    c = parseHelp();
                }
                break;
            case 'i':
            case 'I':
                if (readIf("INSERT")) {
                    c = parseInsert();
                }
                break;
            case 'm':
            case 'M':
                if (readIf("MERGE")) {
                    c = parseMerge();
                }
                break;
            case 'p':
            case 'P':
                if (readIf("PREPARE")) {
                    c = parsePrepare();
                }
                break;
            case 'r':
            case 'R':
                if (readIf("ROLLBACK")) {
                    c = parseRollback();
                } else if (readIf("REVOKE")) {
                    c = parseGrantRevoke(CommandInterface.REVOKE);
                } else if (readIf("RUNSCRIPT")) {
                    c = parseRunScript();
                } else if (readIf("RELEASE")) {
                    c = parseReleaseSavepoint();
                } else if (readIf("REPLACE")) {
                    c = parseReplace();
                }
                break;
            case 's':
            case 'S':
                if (readIf("SET")) {
                    c = parseSet();
                } else if (readIf("SAVEPOINT")) {
                    c = parseSavepoint();
                } else if (readIf("SCRIPT")) {
                    c = parseScript();
                } else if (readIf("SHUTDOWN")) {
                    c = parseShutdown();
                } else if (readIf("SHOW")) {
                    c = parseShow();
                }
                break;
            case 't':
            case 'T':
                if (readIf("TRUNCATE")) {
                    c = parseTruncate();
                }
                break;
            case 'u':
            case 'U':
                if (readIf("UPDATE")) {
                    c = parseUpdate();
                } else if (readIf("USE")) {
                    c = parseUse();
                }
                break;
            }
        }
        if (c == null) {
            throw getSyntaxError();
        }
        if (indexedParameterList != null) {
            for (int i = 0, size = indexedParameterList.size();
                    i < size; i++) {
                if (indexedParameterList.get(i) == null) {
                    indexedParameterList.set(i, new Parameter(i));
                }
            }
            parameters = indexedParameterList;
        }
        if (readIf(OPEN_BRACE)) {
            do {
                int index = (int) readLong() - 1;
                if (index < 0 || index >= parameters.size()) {
                    throw getSyntaxError();
                }
                Parameter p = parameters.get(index);
                if (p == null) {
                    throw getSyntaxError();
                }
                read(COLON);
                Expression expr = readExpression();
                expr = expr.optimize(session);
                p.setValue(expr.getValue(session));
            } while (readIf(COMMA));
            read(CLOSE_BRACE);
            for (Parameter p : parameters) {
                p.checkSet();
            }
            parameters.clear();
        }
        setSQL(c, null, start);
        return c;
    }

    private DbException getSyntaxError() {
        if (expectedList == null || expectedList.isEmpty()) {
            return DbException.getSyntaxError(sqlCommand, parseIndex);
        }
        return DbException.getSyntaxError(sqlCommand, parseIndex,
                StringUtils.join(new StringBuilder(), expectedList, ", ").toString());
    }

    private Prepared parseBackup() {
        BackupCommand command = new BackupCommand(session);
        read("TO");
        command.setFileName(readExpression());
        return command;
    }

    private Prepared parseAnalyze() {
        Analyze command = new Analyze(session);
        if (readIf(TABLE)) {
            Table table = readTableOrView();
            command.setTable(table);
        }
        if (readIf("SAMPLE_SIZE")) {
            command.setTop(readNonNegativeInt());
        }
        return command;
    }

    private TransactionCommand parseBegin() {
        TransactionCommand command;
        if (!readIf("WORK")) {
            readIf("TRANSACTION");
        }
        command = new TransactionCommand(session, CommandInterface.BEGIN);
        return command;
    }

    private TransactionCommand parseCommit() {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session,
                    CommandInterface.COMMIT_TRANSACTION);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        command = new TransactionCommand(session,
                CommandInterface.COMMIT);
        readIf("WORK");
        return command;
    }

    private TransactionCommand parseShutdown() {
        int type = CommandInterface.SHUTDOWN;
        if (readIf("IMMEDIATELY")) {
            type = CommandInterface.SHUTDOWN_IMMEDIATELY;
        } else if (readIf("COMPACT")) {
            type = CommandInterface.SHUTDOWN_COMPACT;
        } else if (readIf("DEFRAG")) {
            type = CommandInterface.SHUTDOWN_DEFRAG;
        } else {
            readIf("SCRIPT");
        }
        return new TransactionCommand(session, type);
    }

    private TransactionCommand parseRollback() {
        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session,
                    CommandInterface.ROLLBACK_TRANSACTION);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        if (readIf("TO")) {
            read("SAVEPOINT");
            command = new TransactionCommand(session,
                    CommandInterface.ROLLBACK_TO_SAVEPOINT);
            command.setSavepointName(readUniqueIdentifier());
        } else {
            readIf("WORK");
            command = new TransactionCommand(session,
                    CommandInterface.ROLLBACK);
        }
        return command;
    }

    private Prepared parsePrepare() {
        if (readIf("COMMIT")) {
            TransactionCommand command = new TransactionCommand(session,
                    CommandInterface.PREPARE_COMMIT);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        String procedureName = readAliasIdentifier();
        if (readIf(OPEN_PAREN)) {
            ArrayList<Column> list = Utils.newSmallArrayList();
            for (int i = 0;; i++) {
                Column column = parseColumnForTable("C" + i, true, false);
                list.add(column);
                if (!readIfMore(true)) {
                    break;
                }
            }
        }
        read("AS");
        Prepared prep = parsePrepared();
        PrepareProcedure command = new PrepareProcedure(session);
        command.setProcedureName(procedureName);
        command.setPrepared(prep);
        return command;
    }

    private TransactionCommand parseSavepoint() {
        TransactionCommand command = new TransactionCommand(session,
                CommandInterface.SAVEPOINT);
        command.setSavepointName(readUniqueIdentifier());
        return command;
    }

    private Prepared parseReleaseSavepoint() {
        Prepared command = new NoOperation(session);
        readIf("SAVEPOINT");
        readUniqueIdentifier();
        return command;
    }

    private Schema findSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = database.findSchema(schemaName);
        if (schema == null) {
            if (equalsToken("SESSION", schemaName)) {
                // for local temporary tables
                schema = database.getSchema(session.getCurrentSchemaName());
            }
        }
        return schema;
    }

    private Schema getSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    private Schema getSchema() {
        return getSchema(schemaName);
    }
    /*
     * Gets the current schema for scenarios that need a guaranteed, non-null schema object.
     *
     * This routine is solely here
     * because of the function readIdentifierWithSchema(String defaultSchemaName) - which
     * is often called with a null parameter (defaultSchemaName) - then 6 lines into the function
     * that routine nullifies the state field schemaName - which I believe is a bug.
     *
     * There are about 7 places where "readIdentifierWithSchema(null)" is called in this file.
     *
     * In other words when is it legal to not have an active schema defined by schemaName ?
     * I don't think it's ever a valid case. I don't understand when that would be allowed.
     * I spent a long time trying to figure this out.
     * As another proof of this point, the command "SET SCHEMA=NULL" is not a valid command.
     *
     * I did try to fix this in readIdentifierWithSchema(String defaultSchemaName)
     * - but every fix I tried cascaded so many unit test errors - so
     * I gave up. I think this needs a bigger effort to fix his, as part of bigger, dedicated story.
     *
     */
    private Schema getSchemaWithDefault() {
        if (schemaName == null) {
            schemaName = session.getCurrentSchemaName();
        }
        return getSchema(schemaName);
    }

    private Column readTableColumn(TableFilter filter) {
        boolean rowId = false;
        String columnName = null;
        if (currentTokenType == _ROWID_) {
            read();
            rowId = true;
        } else {
            columnName = readColumnIdentifier();
            if (readIf(DOT)) {
                String tableAlias = columnName;
                if (currentTokenType == _ROWID_) {
                    read();
                    rowId = true;
                } else {
                    columnName = readColumnIdentifier();
                    if (readIf(DOT)) {
                        String schema = tableAlias;
                        tableAlias = columnName;
                        if (currentTokenType == _ROWID_) {
                            read();
                            rowId = true;
                        } else {
                            columnName = readColumnIdentifier();
                            if (readIf(DOT)) {
                                if (!equalsToken(schema, database.getShortName())) {
                                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, schema);
                                }
                                schema = tableAlias;
                                tableAlias = columnName;
                                if (currentTokenType == _ROWID_) {
                                    read();
                                    rowId = true;
                                } else {
                                    columnName = readColumnIdentifier();
                                }
                            }
                        }
                        if (!equalsToken(schema, filter.getTable().getSchema().getName())) {
                            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schema);
                        }
                    }
                }
                if (!equalsToken(tableAlias, filter.getTableAlias())) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
            }
        }
        return rowId ? filter.getRowIdColumn() : filter.getTable().getColumn(columnName);
    }

    private Update parseUpdate() {
        Update command = new Update(session);
        currentPrepared = command;
        int start = lastParseIndex;
        TableFilter filter = readSimpleTableFilter(0, null);
        command.setTableFilter(filter);
        parseUpdateSetClause(command, filter, start, true);
        return command;
    }

    private void parseUpdateSetClause(Update command, TableFilter filter, int start, boolean allowExtensions) {
        read("SET");
        if (readIf(OPEN_PAREN)) {
            ArrayList<Column> columns = Utils.newSmallArrayList();
            do {
                Column column = readTableColumn(filter);
                columns.add(column);
            } while (readIfMore(true));
            read(EQUAL);
            Expression expression = readExpression();
            if (columns.size() == 1 && expression.getType().getValueType() != Value.ROW) {
                // the expression is parsed as a simple value
                command.setAssignment(columns.get(0), expression);
            } else {
                for (int i = 0, size = columns.size(); i < size; i++) {
                    Column column = columns.get(i);
                    Function f = Function.getFunction(database, "ARRAY_GET");
                    f.setParameter(0, expression);
                    f.setParameter(1, ValueExpression.get(ValueInt.get(i + 1)));
                    f.doneWithParameters();
                    command.setAssignment(column, f);
                }
            }
        } else {
            do {
                Column column = readTableColumn(filter);
                read(EQUAL);
                command.setAssignment(column, readExpressionOrDefault());
            } while (readIf(COMMA));
        }
        if (readIf(WHERE)) {
            Expression condition = readExpression();
            command.setCondition(condition);
        }
        if (allowExtensions) {
            if (readIf(ORDER)) {
                // for MySQL compatibility
                // (this syntax is supported, but ignored)
                read("BY");
                parseSimpleOrderList();
            }
            if (readIf(LIMIT)) {
                Expression limit = readTerm().optimize(session);
                command.setLimit(limit);
            }
        }
        setSQL(command, "UPDATE", start);
    }

    private TableFilter readSimpleTableFilter(int orderInFrom, Collection<String> excludeTokens) {
        Table table = readTableOrView();
        String alias = null;
        if (readIf("AS")) {
            alias = readAliasIdentifier();
        } else if (currentTokenType == IDENTIFIER) {
            if (!equalsTokenIgnoreCase(currentToken, "SET")
                    && (excludeTokens == null || !isTokenInList(excludeTokens))) {
                // SET is not a keyword (PostgreSQL supports it as a table name)
                alias = readAliasIdentifier();
            }
        }
        return new TableFilter(session, table, alias, rightsChecked,
                currentSelect, orderInFrom, null);
    }

    private Delete parseDelete() {
        Delete command = new Delete(session);
        Expression limit = null;
        if (readIf("TOP")) {
            limit = readTerm().optimize(session);
        }
        currentPrepared = command;
        int start = lastParseIndex;
        if (!readIf(FROM) && database.getMode().getEnum() == ModeEnum.MySQL) {
            readIdentifierWithSchema();
            read(FROM);
        }
        TableFilter filter = readSimpleTableFilter(0, null);
        command.setTableFilter(filter);
        if (readIf(WHERE)) {
            command.setCondition(readExpression());
        }
        if (limit == null && readIf(LIMIT)) {
            limit = readTerm().optimize(session);
        }
        command.setLimit(limit);
        setSQL(command, "DELETE", start);
        return command;
    }

    private IndexColumn[] parseIndexColumnList() {
        ArrayList<IndexColumn> columns = Utils.newSmallArrayList();
        do {
            IndexColumn column = new IndexColumn();
            column.columnName = readColumnIdentifier();
            column.sortType = parseSortType();
            columns.add(column);
        } while (readIfMore(true));
        return columns.toArray(new IndexColumn[0]);
    }

    private int parseSortType() {
        int sortType = parseSimpleSortType();
        if (readIf("NULLS")) {
            if (readIf("FIRST")) {
                sortType |= SortOrder.NULLS_FIRST;
            } else {
                read("LAST");
                sortType |= SortOrder.NULLS_LAST;
            }
        }
        return sortType;
    }

    private int parseSimpleSortType() {
        if (!readIf("ASC") && readIf("DESC")) {
            return SortOrder.DESCENDING;
        }
        return SortOrder.ASCENDING;
    }

    private String[] parseColumnList() {
        ArrayList<String> columns = Utils.newSmallArrayList();
        do {
            String columnName = readColumnIdentifier();
            columns.add(columnName);
        } while (readIfMore(false));
        return columns.toArray(new String[0]);
    }

    private Column[] parseColumnList(Table table) {
        ArrayList<Column> columns = Utils.newSmallArrayList();
        HashSet<Column> set = new HashSet<>();
        if (!readIf(CLOSE_PAREN)) {
            do {
                Column column = parseColumn(table);
                if (!set.add(column)) {
                    throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getSQL(false));
                }
                columns.add(column);
            } while (readIfMore(false));
        }
        return columns.toArray(new Column[0]);
    }

    private Column parseColumn(Table table) {
        if (currentTokenType == _ROWID_) {
            read();
            return table.getRowIdColumn();
        }
        return table.getColumn(readColumnIdentifier());
    }

    /**
     * Read comma or closing brace.
     *
     * @param strict
     *            if {@code false} additional comma before brace is allowed
     * @return {@code true} if comma is read, {@code false} if brace is read
     */
    private boolean readIfMore(boolean strict) {
        if (readIf(COMMA)) {
            return strict || !readIf(CLOSE_PAREN);
        }
        read(CLOSE_PAREN);
        return false;
    }

    private Prepared parseHelp() {
        Select select = new Select(session, null);
        select.setWildcard();
        String informationSchema = database.sysIdentifier("INFORMATION_SCHEMA");
        Table table = database.getSchema(informationSchema)
                .resolveTableOrView(session, database.sysIdentifier("HELP"));
        Function function = Function.getFunction(database, "UPPER");
        function.setParameter(0, new ExpressionColumn(database, informationSchema,
                database.sysIdentifier("HELP"), database.sysIdentifier("TOPIC"), false));
        function.doneWithParameters();
        TableFilter filter = new TableFilter(session, table, null, rightsChecked, select, 0, null);
        select.addTableFilter(filter, true);
        while (currentTokenType != END) {
            String s = currentToken;
            read();
            CompareLike like = new CompareLike(database, function,
                    ValueExpression.get(ValueString.get('%' + s + '%')), null, false);
            select.addCondition(like);
        }
        select.init();
        return select;
    }

    private Prepared parseShow() {
        ArrayList<Value> paramValues = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder("SELECT ");
        if (readIf("CLIENT_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UNICODE' AS CLIENT_ENCODING FROM DUAL");
        } else if (readIf("DEFAULT_TRANSACTION_ISOLATION")) {
            // for PostgreSQL compatibility
            buff.append("'read committed' AS DEFAULT_TRANSACTION_ISOLATION " +
                    "FROM DUAL");
        } else if (readIf("TRANSACTION")) {
            // for PostgreSQL compatibility
            read("ISOLATION");
            read("LEVEL");
            buff.append("'read committed' AS TRANSACTION_ISOLATION " +
                    "FROM DUAL");
        } else if (readIf("DATESTYLE")) {
            // for PostgreSQL compatibility
            buff.append("'ISO' AS DATESTYLE FROM DUAL");
        } else if (readIf("SERVER_VERSION")) {
            // for PostgreSQL compatibility
            buff.append("'" + Constants.PG_VERSION + "' AS SERVER_VERSION FROM DUAL");
        } else if (readIf("SERVER_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UTF8' AS SERVER_ENCODING FROM DUAL");
        } else if (readIf("TABLES")) {
            // for MySQL compatibility
            String schema = database.getMainSchema().getName();
            if (readIf(FROM)) {
                schema = readUniqueIdentifier();
            }
            buff.append("TABLE_NAME, TABLE_SCHEMA FROM "
                    + "INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME");
            paramValues.add(ValueString.get(schema));
        } else if (readIf("COLUMNS")) {
            // for MySQL compatibility
            read(FROM);
            String tableName = readIdentifierWithSchema();
            String schemaName = getSchema().getName();
            paramValues.add(ValueString.get(tableName));
            if (readIf(FROM)) {
                schemaName = readUniqueIdentifier();
            }
            buff.append("C.COLUMN_NAME FIELD, "
                    + "C.TYPE_NAME || '(' || C.NUMERIC_PRECISION || ')' TYPE, "
                    + "C.IS_NULLABLE \"NULL\", "
                    + "CASE (SELECT MAX(I.INDEX_TYPE_NAME) FROM "
                    + "INFORMATION_SCHEMA.INDEXES I "
                    + "WHERE I.TABLE_SCHEMA=C.TABLE_SCHEMA "
                    + "AND I.TABLE_NAME=C.TABLE_NAME "
                    + "AND I.COLUMN_NAME=C.COLUMN_NAME)"
                    + "WHEN 'PRIMARY KEY' THEN 'PRI' "
                    + "WHEN 'UNIQUE INDEX' THEN 'UNI' ELSE '' END KEY, "
                    + "IFNULL(COLUMN_DEFAULT, 'NULL') DEFAULT "
                    + "FROM INFORMATION_SCHEMA.COLUMNS C "
                    + "WHERE C.TABLE_NAME=? AND C.TABLE_SCHEMA=? "
                    + "ORDER BY C.ORDINAL_POSITION");
            paramValues.add(ValueString.get(schemaName));
        } else if (readIf("DATABASES") || readIf("SCHEMAS")) {
            // for MySQL compatibility
            buff.append("SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        }
        boolean b = session.getAllowLiterals();
        try {
            // need to temporarily enable it, in case we are in
            // ALLOW_LITERALS_NUMBERS mode
            session.setAllowLiterals(true);
            return prepare(session, buff.toString(), paramValues);
        } finally {
            session.setAllowLiterals(b);
        }
    }

    private static Prepared prepare(Session s, String sql,
            ArrayList<Value> paramValues) {
        Prepared prep = s.prepare(sql);
        ArrayList<Parameter> params = prep.getParameters();
        if (params != null) {
            for (int i = 0, size = params.size(); i < size; i++) {
                Parameter p = params.get(i);
                p.setValue(paramValues.get(i));
            }
        }
        return prep;
    }

    private boolean isSelect() {
        int start = lastParseIndex;
        while (readIf(OPEN_PAREN)) {
            // need to read ahead, it could be a nested union:
            // ((select 1) union (select 1))
        }
        boolean select;
        switch (currentTokenType) {
        case FROM:
        case SELECT:
        case VALUES:
        case WITH:
            select = true;
            break;
        case TABLE:
            read();
            select = !readIf(OPEN_PAREN);
            break;
        default:
            select = false;
        }
        parseIndex = start;
        read();
        return select;
    }

    private Prepared parseMerge() {
        int start = lastParseIndex;
        read("INTO");
        List<String> excludeIdentifiers = Arrays.asList("USING", "KEY");
        TableFilter targetTableFilter = readSimpleTableFilter(0, excludeIdentifiers);
        if (readIf("USING")) {
            return parseMergeUsing(targetTableFilter, start);
        }
        Merge command = new Merge(session);
        currentPrepared = command;
        command.setTargetTableFilter(targetTableFilter);
        Table table = command.getTargetTable();
        if (readIf(OPEN_PAREN)) {
            if (isSelect()) {
                command.setQuery(parseSelect());
                read(CLOSE_PAREN);
                return command;
            }
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf("KEY")) {
            read(OPEN_PAREN);
            Column[] keys = parseColumnList(table);
            command.setKeys(keys);
        }
        if (readIf(VALUES)) {
            parseValuesForCommand(command);
        } else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private MergeUsing parseMergeUsing(TableFilter targetTableFilter, int start) {
        MergeUsing command = new MergeUsing(session, targetTableFilter);
        currentPrepared = command;

        if (readIf(OPEN_PAREN)) {
            /* a select query is supplied */
            if (isSelect()) {
                command.setQuery(parseSelect());
                read(CLOSE_PAREN);
            }
            String queryAlias = readFromAlias(null, null);
            if (queryAlias == null) {
                queryAlias = Constants.PREFIX_QUERY_ALIAS + parseIndex;
            }
            command.setQueryAlias(queryAlias);

            String[] querySQLOutput = {null};
            List<Column> columnTemplateList = TableView.createQueryColumnTemplateList(null, command.getQuery(),
                    querySQLOutput);
            TableView temporarySourceTableView = createCTEView(
                    queryAlias, querySQLOutput[0],
                    columnTemplateList, false/* no recursion */,
                    false/* do not add to session */,
                    true /* isTemporary */
            );
            TableFilter sourceTableFilter = new TableFilter(session,
                    temporarySourceTableView, queryAlias,
                    rightsChecked, (Select) command.getQuery(), 0, null);
            command.setSourceTableFilter(sourceTableFilter);
        } else {
            /* Its a table name, simulate a query by building a select query for the table */
            TableFilter sourceTableFilter = readSimpleTableFilter(0, null);
            command.setSourceTableFilter(sourceTableFilter);

            Select preparedQuery = new Select(session, null);
            preparedQuery.setWildcard();
            TableFilter filter = new TableFilter(session, sourceTableFilter.getTable(),
                    sourceTableFilter.getTableAlias(), rightsChecked, preparedQuery, 0, null);
            preparedQuery.addTableFilter(filter, true);
            preparedQuery.init();
            command.setQuery(preparedQuery);
        }
        read(ON);
        Expression condition = readExpression();
        command.setOnCondition(condition);

        read("WHEN");
        do {
            boolean matched = readIf("MATCHED");
            if (matched) {
                parseWhenMatched(command);
            } else {
                parseWhenNotMatched(command);
            }
        } while (readIf("WHEN"));

        setSQL(command, "MERGE", start);
        return command;
    }

    private void parseWhenMatched(MergeUsing command) {
        Expression and = readIf("AND") ? readExpression() : null;
        read("THEN");
        int startMatched = lastParseIndex;
        Update updateCommand = null;
        if (readIf("UPDATE")) {
            updateCommand = new Update(session);
            TableFilter filter = command.getTargetTableFilter();
            updateCommand.setTableFilter(filter);
            parseUpdateSetClause(updateCommand, filter, startMatched, false);
            startMatched = lastParseIndex;
        }
        Delete deleteCommand = null;
        if (readIf("DELETE")) {
            deleteCommand = new Delete(session);
            deleteCommand.setTableFilter(command.getTargetTableFilter());
            if (readIf(WHERE)) {
                deleteCommand.setCondition(readExpression());
            }
            setSQL(deleteCommand, "DELETE", startMatched);
        }
        if (updateCommand != null || deleteCommand != null) {
            MergeUsing.WhenMatched when = new MergeUsing.WhenMatched(command);
            when.setAndCondition(and);
            when.setUpdateCommand(updateCommand);
            when.setDeleteCommand(deleteCommand);
            command.addWhen(when);
        } else {
            throw getSyntaxError();
        }
    }

    private void parseWhenNotMatched(MergeUsing command) {
        read(NOT);
        read("MATCHED");
        Expression and = readIf("AND") ? readExpression() : null;
        read("THEN");
        if (readIf("INSERT")) {
            Insert insertCommand = new Insert(session);
            insertCommand.setTable(command.getTargetTable());
            parseInsertGivenTable(insertCommand, command.getTargetTable());
            MergeUsing.WhenNotMatched when = new MergeUsing.WhenNotMatched(command);
            when.setAndCondition(and);
            when.setInsertCommand(insertCommand);
            command.addWhen(when);
        } else {
            throw getSyntaxError();
        }
    }

    private Insert parseInsert() {
        Insert command = new Insert(session);
        currentPrepared = command;
        if (database.getMode().onDuplicateKeyUpdate && readIf("IGNORE")) {
            command.setIgnore(true);
        }
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        Insert returnedCommand = parseInsertGivenTable(command, table);
        if (returnedCommand != null) {
            return returnedCommand;
        }
        if (database.getMode().onDuplicateKeyUpdate) {
            if (readIf(ON)) {
                read("DUPLICATE");
                read("KEY");
                read("UPDATE");
                do {
                    String columnName = readColumnIdentifier();
                    if (readIf(DOT)) {
                        String schemaOrTableName = columnName;
                        String tableOrColumnName = readColumnIdentifier();
                        if (readIf(DOT)) {
                            if (!table.getSchema().getName().equals(schemaOrTableName)) {
                                throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH);
                            }
                            columnName = readColumnIdentifier();
                        } else {
                            columnName = tableOrColumnName;
                            tableOrColumnName = schemaOrTableName;
                        }
                        if (!table.getName().equals(tableOrColumnName)) {
                            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableOrColumnName);
                        }
                    }
                    Column column = table.getColumn(columnName);
                    read(EQUAL);
                    command.addAssignmentForDuplicate(column, readExpressionOrDefault());
                } while (readIf(COMMA));
            }
        }
        if (database.getMode().isolationLevelInSelectOrInsertStatement) {
            parseIsolationClause();
        }
        return command;
    }

    private Insert parseInsertGivenTable(Insert command, Table table) {
        Column[] columns = null;
        if (readIf(OPEN_PAREN)) {
            if (isSelect()) {
                command.setQuery(parseSelect());
                read(CLOSE_PAREN);
                return command;
            }
            columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf("DIRECT")) {
            command.setInsertFromSelect(true);
        }
        if (readIf("SORTED")) {
            command.setSortedInsertMode(true);
        }
        if (readIf("DEFAULT")) {
            read(VALUES);
            command.addRow(new Expression[0]);
        } else if (readIf(VALUES)) {
            parseValuesForCommand(command);
        } else if (readIf("SET")) {
            if (columns != null) {
                throw getSyntaxError();
            }
            ArrayList<Column> columnList = Utils.newSmallArrayList();
            ArrayList<Expression> values = Utils.newSmallArrayList();
            do {
                columnList.add(parseColumn(table));
                read(EQUAL);
                values.add(readExpressionOrDefault());
            } while (readIf(COMMA));
            command.setColumns(columnList.toArray(new Column[0]));
            command.addRow(values.toArray(new Expression[0]));
        } else {
            command.setQuery(parseSelect());
        }
        return null;
    }

    /**
     * MySQL compatibility. REPLACE is similar to MERGE.
     */
    private Replace parseReplace() {
        Replace command = new Replace(session);
        currentPrepared = command;
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        if (readIf(OPEN_PAREN)) {
            if (isSelect()) {
                command.setQuery(parseSelect());
                read(CLOSE_PAREN);
                return command;
            }
            Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf(VALUES)) {
            parseValuesForCommand(command);
        } else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private void parseValuesForCommand(CommandWithValues command) {
        ArrayList<Expression> values = Utils.newSmallArrayList();
        do {
            values.clear();
            boolean multiColumn;
            if (readIf(ROW)) {
                read(OPEN_PAREN);
                multiColumn = true;
            } else {
                multiColumn = readIf(OPEN_PAREN);
            }
            if (multiColumn) {
                if (!readIf(CLOSE_PAREN)) {
                    do {
                        values.add(readIf("DEFAULT") ? null : readExpression());
                    } while (readIfMore(false));
                }
            } else {
                values.add(readIf("DEFAULT") ? null : readExpression());
            }
            command.addRow(values.toArray(new Expression[0]));
        } while (readIf(COMMA));
    }

    private TableFilter readTableFilter() {
        Table table;
        String alias = null;
        label: if (readIf(OPEN_PAREN)) {
            if (isSelect()) {
                Query query = parseSelectUnion();
                read(CLOSE_PAREN);
                query.setParameterList(new ArrayList<>(parameters));
                query.init();
                Session s;
                if (createView != null) {
                    s = database.getSystemSession();
                } else {
                    s = session;
                }
                alias = session.getNextSystemIdentifier(sqlCommand);
                table = TableView.createTempView(s, session.getUser(), alias,
                        query, currentSelect);
            } else {
                TableFilter top;
                top = readTableFilter();
                top = readJoin(top);
                read(CLOSE_PAREN);
                alias = readFromAlias(null);
                if (alias != null) {
                    top.setAlias(alias);
                    ArrayList<String> derivedColumnNames = readDerivedColumnNames();
                    if (derivedColumnNames != null) {
                        top.setDerivedColumns(derivedColumnNames);
                    }
                }
                return top;
            }
        } else if (readIf(VALUES)) {
            table = parseValuesTable(0).getTable();
        } else if (readIf(TABLE)) {
            read(OPEN_PAREN);
            table = readTableFunction("TABLE", null, database.getMainSchema());
        } else {
            String tableName = readIdentifierWithSchema(null);
            Schema schema;
            if (schemaName == null) {
                schema = null;
            } else {
                schema = findSchema(schemaName);
                if (schema == null) {
                    if (isDualTable(tableName)) {
                        table = getDualTable(false);
                        break label;
                    }
                    throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
                }
            }
            boolean foundLeftBracket = readIf(OPEN_PAREN);
            if (foundLeftBracket && readIf("INDEX")) {
                // Sybase compatibility with
                // "select * from test (index table1_index)"
                readIdentifierWithSchema(null);
                read(CLOSE_PAREN);
                foundLeftBracket = false;
            }
            if (foundLeftBracket) {
                Schema mainSchema = database.getMainSchema();
                if (equalsToken(tableName, RangeTable.NAME)
                        || equalsToken(tableName, RangeTable.ALIAS)) {
                    Expression min = readExpression();
                    read(COMMA);
                    Expression max = readExpression();
                    if (readIf(COMMA)) {
                        Expression step = readExpression();
                        read(CLOSE_PAREN);
                        table = new RangeTable(mainSchema, min, max, step,
                                false);
                    } else {
                        read(CLOSE_PAREN);
                        table = new RangeTable(mainSchema, min, max, false);
                    }
                } else {
                    table = readTableFunction(tableName, schema, mainSchema);
                }
            } else {
                table = readTableOrView(tableName);
            }
        }
        ArrayList<String> derivedColumnNames = null;
        IndexHints indexHints = null;
        // for backward compatibility, handle case where USE is a table alias
        if (readIf("USE")) {
            if (readIf("INDEX")) {
                indexHints = parseIndexHints(table);
            } else {
                alias = "USE";
                derivedColumnNames = readDerivedColumnNames();
            }
        } else {
            alias = readFromAlias(alias);
            if (alias != null) {
                derivedColumnNames = readDerivedColumnNames();
                // if alias present, a second chance to parse index hints
                if (readIf("USE")) {
                    read("INDEX");
                    indexHints = parseIndexHints(table);
                }
            }
        }

        if (database.getMode().discardWithTableHints) {
            discardWithTableHints();
        }

        // inherit alias for CTE as views from table name
        if (table.isView() && table.isTableExpression() && alias == null) {
            alias = table.getName();
        }
        TableFilter filter = new TableFilter(session, table, alias, rightsChecked,
                currentSelect, orderInFrom++, indexHints);
        if (derivedColumnNames != null) {
            filter.setDerivedColumns(derivedColumnNames);
        }
        return filter;
    }

    private Table readTableFunction(String tableName, Schema schema, Schema mainSchema) {
        Expression expr = readFunction(schema, tableName);
        if (!(expr instanceof FunctionCall)) {
            throw getSyntaxError();
        }
        FunctionCall call = (FunctionCall) expr;
        if (!call.isDeterministic()) {
            recompileAlways = true;
        }
        return new FunctionTable(mainSchema, session, expr, call);
    }

    private IndexHints parseIndexHints(Table table) {
        read(OPEN_PAREN);
        LinkedHashSet<String> indexNames = new LinkedHashSet<>();
        if (!readIf(CLOSE_PAREN)) {
            do {
                String indexName = readIdentifierWithSchema();
                Index index = table.getIndex(indexName);
                indexNames.add(index.getName());
            } while (readIfMore(true));
        }
        return IndexHints.createUseIndexHints(indexNames);
    }

    private String readFromAlias(String alias, List<String> excludeIdentifiers) {
        if (readIf("AS")) {
            alias = readAliasIdentifier();
        } else if (currentTokenType == IDENTIFIER
                && (excludeIdentifiers == null || !isTokenInList(excludeIdentifiers))) {
            alias = readAliasIdentifier();
        }
        return alias;
    }

    private String readFromAlias(String alias) {
        // left and right are not keywords (because they are functions as
        // well)
        List<String> excludeIdentifiers = Arrays.asList("LEFT", "RIGHT");
        return readFromAlias(alias, excludeIdentifiers);
    }

    private ArrayList<String> readDerivedColumnNames() {
        if (readIf(OPEN_PAREN)) {
            ArrayList<String> derivedColumnNames = new ArrayList<>();
            do {
                derivedColumnNames.add(readAliasIdentifier());
            } while (readIfMore(true));
            return derivedColumnNames;
        }
        return null;
    }

    private void discardWithTableHints() {
        if (readIf(WITH)) {
            read(OPEN_PAREN);
            do {
                discardTableHint();
            } while (readIfMore(true));
        }
    }

    private void discardTableHint() {
        if (readIf("INDEX")) {
            if (readIf(OPEN_PAREN)) {
                do {
                    readExpression();
                } while (readIfMore(true));
            } else {
                read(EQUAL);
                readExpression();
            }
        } else {
            readExpression();
        }
    }

    private Prepared parseTruncate() {
        read(TABLE);
        Table table = readTableOrView();
        boolean restart;
        if (readIf("CONTINUE")) {
            read("IDENTITY");
            restart = false;
        } else if (readIf("RESTART")) {
            read("IDENTITY");
            restart = true;
        } else {
            restart = false;
        }
        TruncateTable command = new TruncateTable(session);
        command.setTable(table);
        command.setRestart(restart);
        return command;
    }

    private boolean readIfExists(boolean ifExists) {
        if (readIf(IF)) {
            read(EXISTS);
            ifExists = true;
        }
        return ifExists;
    }

    private Prepared parseComment() {
        int type = 0;
        read(ON);
        boolean column = false;
        if (readIf(TABLE) || readIf("VIEW")) {
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("COLUMN")) {
            column = true;
            type = DbObject.TABLE_OR_VIEW;
        } else if (readIf("CONSTANT")) {
            type = DbObject.CONSTANT;
        } else if (readIf(CONSTRAINT)) {
            type = DbObject.CONSTRAINT;
        } else if (readIf("ALIAS")) {
            type = DbObject.FUNCTION_ALIAS;
        } else if (readIf("INDEX")) {
            type = DbObject.INDEX;
        } else if (readIf("ROLE")) {
            type = DbObject.ROLE;
        } else if (readIf("SCHEMA")) {
            type = DbObject.SCHEMA;
        } else if (readIf("SEQUENCE")) {
            type = DbObject.SEQUENCE;
        } else if (readIf("TRIGGER")) {
            type = DbObject.TRIGGER;
        } else if (readIf("USER")) {
            type = DbObject.USER;
        } else if (readIf("DOMAIN")) {
            type = DbObject.DOMAIN;
        } else {
            throw getSyntaxError();
        }
        SetComment command = new SetComment(session);
        String objectName;
        if (column) {
            // can't use readIdentifierWithSchema() because
            // it would not read schema.table.column correctly
            // if the db name is equal to the schema name
            ArrayList<String> list = Utils.newSmallArrayList();
            do {
                list.add(readUniqueIdentifier());
            } while (readIf(DOT));
            schemaName = session.getCurrentSchemaName();
            if (list.size() == 4) {
                if (!equalsToken(database.getShortName(), list.remove(0))) {
                    throw DbException.getSyntaxError(sqlCommand, parseIndex,
                            "database name");
                }
            }
            if (list.size() == 3) {
                schemaName = list.remove(0);
            }
            if (list.size() != 2) {
                throw DbException.getSyntaxError(sqlCommand, parseIndex,
                        "table.column");
            }
            objectName = list.get(0);
            command.setColumn(true);
            command.setColumnName(list.get(1));
        } else {
            objectName = readIdentifierWithSchema();
        }
        command.setSchemaName(schemaName);
        command.setObjectName(objectName);
        command.setObjectType(type);
        read(IS);
        command.setCommentExpression(readExpression());
        return command;
    }

    private Prepared parseDrop() {
        if (readIf(TABLE)) {
            boolean ifExists = readIfExists(false);
            String tableName = readIdentifierWithSchema();
            DropTable command = new DropTable(session, getSchema());
            command.setTableName(tableName);
            while (readIf(COMMA)) {
                tableName = readIdentifierWithSchema();
                DropTable next = new DropTable(session, getSchema());
                next.setTableName(tableName);
                command.addNextDropTable(next);
            }
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            if (readIf("CASCADE")) {
                command.setDropAction(ConstraintActionType.CASCADE);
                readIf("CONSTRAINTS");
            } else if (readIf("RESTRICT")) {
                command.setDropAction(ConstraintActionType.RESTRICT);
            } else if (readIf("IGNORE")) {
                command.setDropAction(ConstraintActionType.SET_DEFAULT);
            }
            return command;
        } else if (readIf("INDEX")) {
            boolean ifExists = readIfExists(false);
            String indexName = readIdentifierWithSchema();
            DropIndex command = new DropIndex(session, getSchema());
            command.setIndexName(indexName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            //Support for MySQL: DROP INDEX index_name ON tbl_name
            if (readIf(ON)) {
                readIdentifierWithSchema();
            }
            return command;
        } else if (readIf("USER")) {
            boolean ifExists = readIfExists(false);
            DropUser command = new DropUser(session);
            command.setUserName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            readIf("CASCADE");
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("SEQUENCE")) {
            boolean ifExists = readIfExists(false);
            String sequenceName = readIdentifierWithSchema();
            DropSequence command = new DropSequence(session, getSchema());
            command.setSequenceName(sequenceName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("CONSTANT")) {
            boolean ifExists = readIfExists(false);
            String constantName = readIdentifierWithSchema();
            DropConstant command = new DropConstant(session, getSchema());
            command.setConstantName(constantName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("TRIGGER")) {
            boolean ifExists = readIfExists(false);
            String triggerName = readIdentifierWithSchema();
            DropTrigger command = new DropTrigger(session, getSchema());
            command.setTriggerName(triggerName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("VIEW")) {
            boolean ifExists = readIfExists(false);
            String viewName = readIdentifierWithSchema();
            DropView command = new DropView(session, getSchema());
            command.setViewName(viewName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            ConstraintActionType dropAction = parseCascadeOrRestrict();
            if (dropAction != null) {
                command.setDropAction(dropAction);
            }
            return command;
        } else if (readIf("ROLE")) {
            boolean ifExists = readIfExists(false);
            DropRole command = new DropRole(session);
            command.setRoleName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("ALIAS")) {
            boolean ifExists = readIfExists(false);
            String aliasName = readIdentifierWithSchema();
            DropFunctionAlias command = new DropFunctionAlias(session,
                    getSchema());
            command.setAliasName(aliasName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        } else if (readIf("SCHEMA")) {
            boolean ifExists = readIfExists(false);
            DropSchema command = new DropSchema(session);
            command.setSchemaName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            ConstraintActionType dropAction = parseCascadeOrRestrict();
            if (dropAction != null) {
                command.setDropAction(dropAction);
            }
            return command;
        } else if (readIf(ALL)) {
            read("OBJECTS");
            DropDatabase command = new DropDatabase(session);
            command.setDropAllObjects(true);
            if (readIf("DELETE")) {
                read("FILES");
                command.setDeleteFiles(true);
            }
            return command;
        } else if (readIf("DOMAIN") || readIf("TYPE") || readIf("DATATYPE")) {
            return parseDropDomain();
        } else if (readIf("AGGREGATE")) {
            return parseDropAggregate();
        } else if (readIf("SYNONYM")) {
            boolean ifExists = readIfExists(false);
            String synonymName = readIdentifierWithSchema();
            DropSynonym command = new DropSynonym(session, getSchema());
            command.setSynonymName(synonymName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        throw getSyntaxError();
    }

    private DropDomain parseDropDomain() {
        boolean ifExists = readIfExists(false);
        DropDomain command = new DropDomain(session);
        command.setTypeName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        ConstraintActionType dropAction = parseCascadeOrRestrict();
        if (dropAction != null) {
            command.setDropAction(dropAction);
        }
        return command;
    }

    private DropAggregate parseDropAggregate() {
        boolean ifExists = readIfExists(false);
        DropAggregate command = new DropAggregate(session);
        command.setName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private TableFilter readJoin(TableFilter top) {
        TableFilter last = top;
        while (true) {
            TableFilter join;
            if (readIf("RIGHT")) {
                readIf("OUTER");
                read(JOIN);
                // the right hand side is the 'inner' table usually
                join = readTableFilter();
                join = readJoin(join);
                Expression on = null;
                if (readIf(ON)) {
                    on = readExpression();
                }
                addJoin(join, top, true, on);
                top = join;
            } else if (readIf("LEFT")) {
                readIf("OUTER");
                read(JOIN);
                join = readTableFilter();
                join = readJoin(join);
                Expression on = null;
                if (readIf(ON)) {
                    on = readExpression();
                }
                addJoin(top, join, true, on);
            } else if (readIf(FULL)) {
                throw getSyntaxError();
            } else if (readIf(INNER)) {
                read(JOIN);
                join = readTableFilter();
                top = readJoin(top);
                Expression on = null;
                if (readIf(ON)) {
                    on = readExpression();
                }
                addJoin(top, join, false, on);
            } else if (readIf(JOIN)) {
                join = readTableFilter();
                top = readJoin(top);
                Expression on = null;
                if (readIf(ON)) {
                    on = readExpression();
                }
                addJoin(top, join, false, on);
            } else if (readIf(CROSS)) {
                read(JOIN);
                join = readTableFilter();
                addJoin(top, join, false, null);
            } else if (readIf(NATURAL)) {
                read(JOIN);
                join = readTableFilter();
                Column[] tableCols = last.getTable().getColumns();
                Column[] joinCols = join.getTable().getColumns();
                String tableSchema = last.getTable().getSchema().getName();
                String joinSchema = join.getTable().getSchema().getName();
                Expression on = null;
                for (Column tc : tableCols) {
                    String tableColumnName = tc.getName();
                    for (Column c : joinCols) {
                        String joinColumnName = c.getName();
                        if (equalsToken(tableColumnName, joinColumnName)) {
                            join.addNaturalJoinColumn(c);
                            Expression tableExpr = new ExpressionColumn(
                                    database, tableSchema,
                                    last.getTableAlias(), tableColumnName, false);
                            Expression joinExpr = new ExpressionColumn(
                                    database, joinSchema, join.getTableAlias(),
                                    joinColumnName, false);
                            Expression equal = new Comparison(session,
                                    Comparison.EQUAL, tableExpr, joinExpr);
                            if (on == null) {
                                on = equal;
                            } else {
                                on = new ConditionAndOr(ConditionAndOr.AND, on,
                                        equal);
                            }
                        }
                    }
                }
                addJoin(top, join, false, on);
            } else {
                break;
            }
            last = join;
        }
        return top;
    }

    /**
     * Add one join to another. This method creates nested join between them if
     * required.
     *
     * @param top parent join
     * @param join child join
     * @param outer if child join is an outer join
     * @param on the join condition
     * @see TableFilter#addJoin(TableFilter, boolean, Expression)
     */
    private void addJoin(TableFilter top, TableFilter join, boolean outer, Expression on) {
        if (join.getJoin() != null) {
            String joinTable = Constants.PREFIX_JOIN + parseIndex;
            TableFilter n = new TableFilter(session, getDualTable(true),
                    joinTable, rightsChecked, currentSelect, join.getOrderInFrom(),
                    null);
            n.setNestedJoin(join);
            join = n;
        }
        top.addJoin(join, outer, on);
    }

    private Prepared parseExecute() {
        ExecuteProcedure command = new ExecuteProcedure(session);
        String procedureName = readAliasIdentifier();
        Procedure p = session.getProcedure(procedureName);
        if (p == null) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1,
                    procedureName);
        }
        command.setProcedure(p);
        if (readIf(OPEN_PAREN)) {
            for (int i = 0;; i++) {
                command.setExpression(i, readExpression());
                if (!readIfMore(true)) {
                    break;
                }
            }
        }
        return command;
    }

    private DeallocateProcedure parseDeallocate() {
        readIf("PLAN");
        String procedureName = readAliasIdentifier();
        DeallocateProcedure command = new DeallocateProcedure(session);
        command.setProcedureName(procedureName);
        return command;
    }

    private Explain parseExplain() {
        Explain command = new Explain(session);
        if (readIf("ANALYZE")) {
            command.setExecuteCommand(true);
        } else {
            if (readIf("PLAN")) {
                readIf(FOR);
            }
        }
        switch (currentTokenType) {
        case FROM:
        case SELECT:
        case TABLE:
        case VALUES:
        case WITH:
        case OPEN_PAREN:
            Query query = parseSelect();
            query.setNeverLazy(true);
            command.setCommand(query);
            break;
        default:
            if (readIf("DELETE")) {
                command.setCommand(parseDelete());
            } else if (readIf("UPDATE")) {
                command.setCommand(parseUpdate());
            } else if (readIf("INSERT")) {
                command.setCommand(parseInsert());
            } else if (readIf("MERGE")) {
                command.setCommand(parseMerge());
            } else {
                throw getSyntaxError();
            }
        }
        return command;
    }

    private Query parseSelect() {
        int paramIndex = parameters.size();
        Query command = parseSelectUnion();
        int size = parameters.size();
        ArrayList<Parameter> params = new ArrayList<>(size);
        for (int i = paramIndex; i < size; i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        command.init();
        return command;
    }

    private Prepared parseWithStatementOrQuery() {
        int paramIndex = parameters.size();
        Prepared command = parseWith();
        int size = parameters.size();
        ArrayList<Parameter> params = new ArrayList<>(size);
        for (int i = paramIndex; i < size; i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        if (command instanceof Query) {
            Query query = (Query) command;
            query.init();
        }
        return command;
    }

    private Query parseSelectUnion() {
        int start = lastParseIndex;
        Query command = parseSelectSub();
        for (;;) {
            SelectUnion.UnionType type;
            if (readIf(UNION)) {
                if (readIf(ALL)) {
                    type = SelectUnion.UnionType.UNION_ALL;
                } else {
                    readIf(DISTINCT);
                    type = SelectUnion.UnionType.UNION;
                }
            } else if (readIf(EXCEPT) || readIf(MINUS)) {
                type = SelectUnion.UnionType.EXCEPT;
            } else if (readIf(INTERSECT)) {
                type = SelectUnion.UnionType.INTERSECT;
            } else {
                break;
            }
            command = new SelectUnion(session, type, command, parseSelectSub());
        }
        parseEndOfQuery(command);
        setSQL(command, null, start);
        return command;
    }

    private void parseEndOfQuery(Query command) {
        if (readIf(ORDER)) {
            read("BY");
            Select oldSelect = currentSelect;
            if (command instanceof Select) {
                currentSelect = (Select) command;
            }
            ArrayList<SelectOrderBy> orderList = Utils.newSmallArrayList();
            do {
                boolean canBeNumber = !readIf(EQUAL);
                SelectOrderBy order = new SelectOrderBy();
                Expression expr = readExpression();
                if (canBeNumber && expr instanceof ValueExpression && expr.getType().getValueType() == Value.INT) {
                    order.columnIndexExpr = expr;
                } else if (expr instanceof Parameter) {
                    recompileAlways = true;
                    order.columnIndexExpr = expr;
                } else {
                    order.expression = expr;
                }
                order.sortType = parseSortType();
                orderList.add(order);
            } while (readIf(COMMA));
            command.setOrder(orderList);
            currentSelect = oldSelect;
        }
        if (command.getLimit() == null) {
            // make sure aggregate functions will not work here
            Select temp = currentSelect;
            currentSelect = null;
            boolean hasOffsetOrFetch = false;
            // Standard SQL OFFSET / FETCH
            if (readIf(OFFSET)) {
                hasOffsetOrFetch = true;
                command.setOffset(readExpression().optimize(session));
                if (!readIf(ROW)) {
                    readIf("ROWS");
                }
            }
            if (readIf(FETCH)) {
                hasOffsetOrFetch = true;
                if (!readIf("FIRST")) {
                    read("NEXT");
                }
                if (readIf(ROW) || readIf("ROWS")) {
                    command.setLimit(ValueExpression.get(ValueInt.get(1)));
                } else {
                    Expression limit = readExpression().optimize(session);
                    command.setLimit(limit);
                    if (readIf("PERCENT")) {
                        command.setFetchPercent(true);
                    }
                    if (!readIf(ROW)) {
                        read("ROWS");
                    }
                }
                if (readIf(WITH)) {
                    read("TIES");
                    command.setWithTies(true);
                } else {
                    read("ONLY");
                }
            }
            // MySQL-style LIMIT / OFFSET
            if (!hasOffsetOrFetch && readIf(LIMIT)) {
                Expression limit = readExpression().optimize(session);
                command.setLimit(limit);
                if (readIf(OFFSET)) {
                    Expression offset = readExpression().optimize(session);
                    command.setOffset(offset);
                } else if (readIf(COMMA)) {
                    // MySQL: [offset, ] rowcount
                    Expression offset = limit;
                    limit = readExpression().optimize(session);
                    command.setOffset(offset);
                    command.setLimit(limit);
                }
            }
            if (readIf("SAMPLE_SIZE")) {
                Expression sampleSize = readExpression().optimize(session);
                command.setSampleSize(sampleSize);
            }
            currentSelect = temp;
        }
        if (readIf(FOR)) {
            if (readIf("UPDATE")) {
                if (readIf("OF")) {
                    do {
                        readIdentifierWithSchema();
                    } while (readIf(COMMA));
                } else if (readIf("NOWAIT")) {
                    // TODO parser: select for update nowait: should not wait
                }
                command.setForUpdate(true);
            } else if (readIf("READ") || readIf(FETCH)) {
                read("ONLY");
            }
        }
        if (database.getMode().isolationLevelInSelectOrInsertStatement) {
            parseIsolationClause();
        }
    }

    /**
     * DB2 isolation clause
     */
    private void parseIsolationClause() {
        if (readIf(WITH)) {
            if (readIf("RR") || readIf("RS")) {
                // concurrent-access-resolution clause
                if (readIf("USE")) {
                    read("AND");
                    read("KEEP");
                    if (readIf("SHARE") || readIf("UPDATE") ||
                            readIf("EXCLUSIVE")) {
                        // ignore
                    }
                    read("LOCKS");
                }
            } else if (readIf("CS") || readIf("UR")) {
                // ignore
            }
        }
    }

    private Query parseSelectSub() {
        if (readIf(OPEN_PAREN)) {
            Query command = parseSelectUnion();
            read(CLOSE_PAREN);
            return command;
        }
        if (readIf(WITH)) {
            Query query;
            try {
                query = (Query) parseWith();
            } catch (ClassCastException e) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                        "WITH statement supports only SELECT (query) in this context");
            }
            // recursive can not be lazy
            query.setNeverLazy(true);
            return query;
        }
        return parseSelectSimple();
    }

    private void parseSelectSimpleFromPart(Select command) {
        do {
            TableFilter filter = readTableFilter();
            parseJoinTableFilter(filter, command);
        } while (readIf(COMMA));

        // Parser can reorder joined table filters, need to explicitly sort them
        // to get the order as it was in the original query.
        if (session.isForceJoinOrder()) {
            Collections.sort(command.getTopFilters(), TABLE_FILTER_COMPARATOR);
        }
    }

    private void parseJoinTableFilter(TableFilter top, final Select command) {
        top = readJoin(top);
        command.addTableFilter(top, true);
        boolean isOuter = false;
        while (true) {
            TableFilter n = top.getNestedJoin();
            if (n != null) {
                n.visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        command.addTableFilter(f, false);
                    }
                });
            }
            TableFilter join = top.getJoin();
            if (join == null) {
                break;
            }
            isOuter = isOuter | join.isJoinOuter();
            if (isOuter) {
                command.addTableFilter(join, false);
            } else {
                // make flat so the optimizer can work better
                Expression on = join.getJoinCondition();
                if (on != null) {
                    command.addCondition(on);
                }
                join.removeJoinCondition();
                top.removeJoin();
                command.addTableFilter(join, true);
            }
            top = join;
        }
    }

    private void parseSelectSimpleSelectPart(Select command) {
        Select temp = currentSelect;
        // make sure aggregate functions will not work in TOP and LIMIT
        currentSelect = null;
        if (readIf("TOP")) {
            // can't read more complex expressions here because
            // SELECT TOP 1 +? A FROM TEST could mean
            // SELECT TOP (1+?) A FROM TEST or
            // SELECT TOP 1 (+?) AS A FROM TEST
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
            if (readIf("PERCENT")) {
                command.setFetchPercent(true);
            }
            if (readIf(WITH)) {
                read("TIES");
                command.setWithTies(true);
            }
        } else if (readIf(LIMIT)) {
            Expression offset = readTerm().optimize(session);
            command.setOffset(offset);
            Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        }
        currentSelect = temp;
        if (readIf(DISTINCT)) {
            if (readIf(ON)) {
                read(OPEN_PAREN);
                ArrayList<Expression> distinctExpressions = Utils.newSmallArrayList();
                do {
                    distinctExpressions.add(readExpression());
                } while (readIfMore(true));
                command.setDistinct(distinctExpressions.toArray(new Expression[0]));
            } else {
                command.setDistinct();
            }
        } else {
            readIf(ALL);
        }
        ArrayList<Expression> expressions = Utils.newSmallArrayList();
        do {
            if (readIf(ASTERISK)) {
                expressions.add(parseWildcard(null, null));
            } else {
                Expression expr = readExpression();
                if (readIf("AS") || currentTokenType == IDENTIFIER) {
                    String alias = readAliasIdentifier();
                    boolean aliasColumnName = database.getSettings().aliasColumnName;
                    aliasColumnName |= database.getMode().aliasColumnName;
                    expr = new Alias(expr, alias, aliasColumnName);
                }
                expressions.add(expr);
            }
        } while (readIf(COMMA));
        command.setExpressions(expressions);
    }

    private Select parseSelectSimple() {
        boolean fromFirst;
        if (readIf(SELECT)) {
            fromFirst = false;
        } else if (readIf(FROM)) {
            fromFirst = true;
        } else if (readIf(TABLE)) {
            int start = lastParseIndex;
            Table table = readTableOrView();
            Select command = new Select(session, currentSelect);
            TableFilter filter = new TableFilter(session, table, null, rightsChecked,
                    command, orderInFrom++, null);
            command.addTableFilter(filter, true);
            ArrayList<Expression> expressions = new ArrayList<>();
            expressions.add(new Wildcard(null, null));
            command.setExpressions(expressions);
            setSQL(command, "TABLE", start);
            return command;
        } else if (readIf(VALUES)) {
            return parseValues();
        } else {
            throw getSyntaxError();
        }
        Select command = new Select(session, currentSelect);
        int start = lastParseIndex;
        Select oldSelect = currentSelect;
        Prepared oldPrepared = currentPrepared;
        currentSelect = command;
        currentPrepared = command;
        if (fromFirst) {
            parseSelectSimpleFromPart(command);
            read(SELECT);
            parseSelectSimpleSelectPart(command);
        } else {
            parseSelectSimpleSelectPart(command);
            if (!readIf(FROM)) {
                // select without FROM: convert to SELECT ... FROM
                // SYSTEM_RANGE(1,1)
                Table dual = getDualTable(false);
                TableFilter filter = new TableFilter(session, dual, null,
                        rightsChecked, currentSelect, 0,
                        null);
                command.addTableFilter(filter, true);
            } else {
                parseSelectSimpleFromPart(command);
            }
        }
        if (readIf(WHERE)) {
            Expression condition = readExpression();
            command.addCondition(condition);
        }
        // the group by is read for the outer select (or not a select)
        // so that columns that are not grouped can be used
        currentSelect = oldSelect;
        if (readIf(GROUP)) {
            read("BY");
            command.setGroupQuery();
            ArrayList<Expression> list = Utils.newSmallArrayList();
            do {
                Expression expr = readExpression();
                list.add(expr);
            } while (readIf(COMMA));
            command.setGroupBy(list);
        }
        currentSelect = command;
        if (readIf(HAVING)) {
            command.setGroupQuery();
            Expression condition = readExpression();
            command.setHaving(condition);
        }
        if (readIf(WINDOW)) {
            do {
                int index = parseIndex;
                String name = readAliasIdentifier();
                read("AS");
                Window w = readWindowSpecification();
                if (!currentSelect.addWindow(name, w)) {
                    throw DbException.getSyntaxError(sqlCommand, index, "unique identifier");
                }
            } while (readIf(COMMA));
        }
        if (readIf(QUALIFY)) {
            command.setWindowQuery();
            Expression condition = readExpression();
            command.setQualify(condition);
        }
        command.setParameterList(parameters);
        currentSelect = oldSelect;
        currentPrepared = oldPrepared;
        setSQL(command, "SELECT", start);
        return command;
    }

    private Table getDualTable(boolean noColumns) {
        Schema main = database.getMainSchema();
        Expression one = ValueExpression.get(ValueLong.get(1));
        return new RangeTable(main, one, one, noColumns);
    }

    private void setSQL(Prepared command, String start, int startIndex) {
        int endIndex = lastParseIndex;
        String sql;
        if (start != null) {
            StringBuilder builder = new StringBuilder(start.length() + endIndex - startIndex + 1)
                    .append(start).append(' ');
            sql = StringUtils.trimSubstring(builder, originalSQL, startIndex, endIndex).toString();
        } else {
            sql = StringUtils.trimSubstring(originalSQL, startIndex, endIndex);
        }
        command.setSQL(sql);
    }

    private Expression readExpressionOrDefault() {
        if (readIf("DEFAULT")) {
            return ValueExpression.getDefault();
        }
        return readExpression();
    }

    private Expression readExpression() {
        Expression r = readAnd();
        while (readIf("OR")) {
            r = new ConditionAndOr(ConditionAndOr.OR, r, readAnd());
        }
        return r;
    }

    private Expression readAnd() {
        Expression r = readCondition();
        while (readIf("AND")) {
            r = new ConditionAndOr(ConditionAndOr.AND, r, readCondition());
        }
        return r;
    }

    private Expression readCondition() {
        if (readIf(NOT)) {
            return new ConditionNot(readCondition());
        }
        if (readIf(EXISTS)) {
            read(OPEN_PAREN);
            Query query = parseSelect();
            // can not reduce expression because it might be a union except
            // query with distinct
            read(CLOSE_PAREN);
            return new ConditionExists(query);
        }
        if (readIf(INTERSECTS)) {
            read(OPEN_PAREN);
            Expression r1 = readConcat();
            read(COMMA);
            Expression r2 = readConcat();
            read(CLOSE_PAREN);
            return new Comparison(session, Comparison.SPATIAL_INTERSECTS, r1,
                    r2);
        }
        Expression r = readConcat();
        while (true) {
            // special case: NOT NULL is not part of an expression (as in CREATE
            // TABLE TEST(ID INT DEFAULT 0 NOT NULL))
            int backup = parseIndex;
            boolean not = readIf(NOT);
            if (not && isToken(NULL)) {
                // this really only works for NOT NULL!
                parseIndex = backup;
                currentToken = "NOT";
                currentTokenType = NOT;
                break;
            }
            if (readIf(LIKE)) {
                Expression b = readConcat();
                Expression esc = null;
                if (readIf("ESCAPE")) {
                    esc = readConcat();
                }
                recompileAlways = true;
                r = new CompareLike(database, r, b, esc, false);
            } else if (readIf("ILIKE")) {
                Function function = Function.getFunction(database, "CAST");
                function.setDataType(new Column("X", Value.STRING_IGNORECASE));
                function.setParameter(0, r);
                r = function;
                Expression b = readConcat();
                Expression esc = null;
                if (readIf("ESCAPE")) {
                    esc = readConcat();
                }
                recompileAlways = true;
                r = new CompareLike(database, r, b, esc, false);
            } else if (readIf("REGEXP")) {
                Expression b = readConcat();
                recompileAlways = true;
                r = new CompareLike(database, r, b, null, true);
            } else if (readIf(IS)) {
                if (readIf(NOT)) {
                    if (readIf(NULL)) {
                        r = new Comparison(session, Comparison.IS_NOT_NULL, r,
                                null);
                    } else if (readIf(DISTINCT)) {
                        read(FROM);
                        r = new Comparison(session, Comparison.EQUAL_NULL_SAFE,
                                r, readConcat());
                    } else {
                        r = new Comparison(session,
                                Comparison.NOT_EQUAL_NULL_SAFE, r, readConcat());
                    }
                } else if (readIf(NULL)) {
                    r = new Comparison(session, Comparison.IS_NULL, r, null);
                } else if (readIf(DISTINCT)) {
                    read(FROM);
                    r = new Comparison(session, Comparison.NOT_EQUAL_NULL_SAFE,
                            r, readConcat());
                } else {
                    r = new Comparison(session, Comparison.EQUAL_NULL_SAFE, r,
                            readConcat());
                }
            } else if (readIf("IN")) {
                read(OPEN_PAREN);
                if (readIf(CLOSE_PAREN)) {
                    if (database.getMode().prohibitEmptyInPredicate) {
                        throw getSyntaxError();
                    }
                    r = ValueExpression.get(ValueBoolean.FALSE);
                } else {
                    if (isSelect()) {
                        Query query = parseSelect();
                        r = new ConditionInSelect(database, r, query, false,
                                Comparison.EQUAL);
                    } else {
                        ArrayList<Expression> v = Utils.newSmallArrayList();
                        Expression last;
                        do {
                            last = readExpression();
                            v.add(last);
                        } while (readIf(COMMA));
                        if (v.size() == 1 && (last instanceof Subquery)) {
                            Subquery s = (Subquery) last;
                            Query q = s.getQuery();
                            r = new ConditionInSelect(database, r, q, false,
                                    Comparison.EQUAL);
                        } else {
                            r = new ConditionIn(database, r, v);
                        }
                    }
                    read(CLOSE_PAREN);
                }
            } else if (readIf("BETWEEN")) {
                Expression low = readConcat();
                read("AND");
                Expression high = readConcat();
                Expression condLow = new Comparison(session,
                        Comparison.SMALLER_EQUAL, low, r);
                Expression condHigh = new Comparison(session,
                        Comparison.BIGGER_EQUAL, high, r);
                r = new ConditionAndOr(ConditionAndOr.AND, condLow, condHigh);
            } else {
                if (not) {
                    throw getSyntaxError();
                }
                int compareType = getCompareType(currentTokenType);
                if (compareType < 0) {
                    break;
                }
                read();
                int start = lastParseIndex;
                if (readIf(ALL)) {
                    read(OPEN_PAREN);
                    if (isSelect()) {
                        Query query = parseSelect();
                        r = new ConditionInSelect(database, r, query, true, compareType);
                        read(CLOSE_PAREN);
                    } else {
                        parseIndex = start;
                        read();
                        r = new Comparison(session, compareType, r, readConcat());
                    }
                } else if (readIf("ANY") || readIf("SOME")) {
                    read(OPEN_PAREN);
                    if (currentTokenType == PARAMETER && compareType == 0) {
                        Parameter p = readParameter();
                        r = new ConditionInParameter(database, r, p);
                        read(CLOSE_PAREN);
                    } else if (isSelect()) {
                        Query query = parseSelect();
                        r = new ConditionInSelect(database, r, query, false, compareType);
                        read(CLOSE_PAREN);
                    } else {
                        parseIndex = start;
                        read();
                        r = new Comparison(session, compareType, r, readConcat());
                    }
                } else {
                    r = new Comparison(session, compareType, r, readConcat());
                }
            }
            if (not) {
                r = new ConditionNot(r);
            }
        }
        return r;
    }

    private Expression readConcat() {
        Expression r = readSum();
        while (true) {
            if (readIf(STRING_CONCAT)) {
                r = new BinaryOperation(OpType.CONCAT, r, readSum());
            } else if (readIf(TILDE)) {
                if (readIf(ASTERISK)) {
                    Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X",
                            Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new CompareLike(database, r, readSum(), null, true);
            } else if (readIf(NOT_TILDE)) {
                if (readIf(ASTERISK)) {
                    Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X",
                            Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new ConditionNot(new CompareLike(database, r, readSum(),
                        null, true));
            } else {
                return r;
            }
        }
    }

    private Expression readSum() {
        Expression r = readFactor();
        while (true) {
            if (readIf(PLUS_SIGN)) {
                r = new BinaryOperation(OpType.PLUS, r, readFactor());
            } else if (readIf(MINUS_SIGN)) {
                r = new BinaryOperation(OpType.MINUS, r, readFactor());
            } else {
                return r;
            }
        }
    }

    private Expression readFactor() {
        Expression r = readTerm();
        while (true) {
            if (readIf(ASTERISK)) {
                r = new BinaryOperation(OpType.MULTIPLY, r, readTerm());
            } else if (readIf(SLASH)) {
                r = new BinaryOperation(OpType.DIVIDE, r, readTerm());
            } else if (readIf(PERCENT)) {
                r = new BinaryOperation(OpType.MODULUS, r, readTerm());
            } else {
                return r;
            }
        }
    }

    private Expression readAggregate(AggregateType aggregateType, String aggregateName) {
        if (currentSelect == null) {
            throw getSyntaxError();
        }
        Aggregate r;
        switch (aggregateType) {
        case COUNT:
            if (readIf(ASTERISK)) {
                r = new Aggregate(AggregateType.COUNT_ALL, new Expression[0], currentSelect, false);
            } else {
                boolean distinct = readDistinctAgg();
                Expression on = readExpression();
                if (on instanceof Wildcard && !distinct) {
                    // PostgreSQL compatibility: count(t.*)
                    r = new Aggregate(AggregateType.COUNT_ALL, new Expression[0], currentSelect, false);
                } else {
                    r = new Aggregate(AggregateType.COUNT, new Expression[] { on }, currentSelect, distinct);
                }
            }
            break;
        case LISTAGG: {
            boolean distinct = readDistinctAgg();
            Expression arg = readExpression(), separator = null;
            ArrayList<SelectOrderBy> orderByList = null;
            if (equalsToken("STRING_AGG", aggregateName)) {
                // PostgreSQL compatibility: string_agg(expression, delimiter)
                read(COMMA);
                separator = readExpression();
                if (readIf(ORDER)) {
                    read("BY");
                    orderByList = parseSimpleOrderList();
                }
            } else if (equalsToken("GROUP_CONCAT", aggregateName)){
                if (readIf(ORDER)) {
                    read("BY");
                    orderByList = parseSimpleOrderList();
                }
                if (readIf("SEPARATOR")) {
                    separator = readExpression();
                }
            } else {
                if (readIf(COMMA)) {
                    separator = readExpression();
                }
                if (readIf(ON)) {
                    read("OVERFLOW");
                    read("ERROR");
                }
            }
            Expression[] args = separator == null ? new Expression[] { arg } : new Expression[] { arg, separator };
            int index = lastParseIndex;
            read(CLOSE_PAREN);
            if (orderByList == null && isToken("WITHIN")) {
                r = readWithinGroup(aggregateType, args, distinct, false);
            } else {
                parseIndex = index;
                read();
                r = new Aggregate(AggregateType.LISTAGG, args, currentSelect, distinct);
                if (orderByList != null) {
                    r.setOrderByList(orderByList);
                }
            }
            break;
        }
        case ARRAY_AGG: {
            boolean distinct = readDistinctAgg();
            r = new Aggregate(AggregateType.ARRAY_AGG, new Expression[] { readExpression() }, currentSelect, distinct);
            if (readIf(ORDER)) {
                read("BY");
                r.setOrderByList(parseSimpleOrderList());
            }
            break;
        }
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST: {
            if (isToken(CLOSE_PAREN)) {
                return readWindowFunction(aggregateName);
            }
            ArrayList<Expression> expressions = Utils.newSmallArrayList();
            do {
                expressions.add(readExpression());
            } while (readIfMore(true));
            r = readWithinGroup(aggregateType, expressions.toArray(new Expression[0]), false, true);
            break;
        }
        case PERCENTILE_CONT:
        case PERCENTILE_DISC: {
            Expression num = readExpression();
            read(CLOSE_PAREN);
            r = readWithinGroup(aggregateType, new Expression[] { num }, false, false);
            break;
        }
        case MODE: {
            if (readIf(CLOSE_PAREN)) {
                r = readWithinGroup(AggregateType.MODE, new Expression[0], false, false);
            } else {
                Expression expr = readExpression();
                r = new Aggregate(aggregateType, new Expression[0], currentSelect, false);
                if (readIf(ORDER)) {
                    read("BY");
                    Expression expr2 = readExpression();
                    String sql = expr.getSQL(true), sql2 = expr2.getSQL(true);
                    if (!sql.equals(sql2)) {
                        throw DbException.getSyntaxError(ErrorCode.IDENTICAL_EXPRESSIONS_SHOULD_BE_USED, sqlCommand,
                                lastParseIndex, sql, sql2);
                    }
                    readAggregateOrder(r, expr, true);
                } else {
                    readAggregateOrder(r, expr, false);
                }
            }
            break;
        }
        default:
            boolean distinct = readDistinctAgg();
            r = new Aggregate(aggregateType, new Expression[] { readExpression() }, currentSelect, distinct);
            break;
        }
        read(CLOSE_PAREN);
        readFilterAndOver(r);
        return r;
    }

    private Aggregate readWithinGroup(AggregateType aggregateType, Expression[] args, boolean distinct,
            boolean forHypotheticalSet) {
        read("WITHIN");
        read(GROUP);
        read(OPEN_PAREN);
        read(ORDER);
        read("BY");
        Aggregate r = new Aggregate(aggregateType, args, currentSelect, distinct);
        if (forHypotheticalSet) {
            int count = args.length;
            ArrayList<SelectOrderBy> orderList = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    read(COMMA);
                }
                SelectOrderBy order = new SelectOrderBy();
                order.expression = readExpression();
                order.sortType = parseSimpleSortType();
                orderList.add(order);
            }
            r.setOrderByList(orderList);
        } else {
            readAggregateOrder(r, readExpression(), true);
        }
        return r;
    }

    private void readAggregateOrder(Aggregate r, Expression expr, boolean parseSortType) {
        ArrayList<SelectOrderBy> orderList = new ArrayList<>(1);
        SelectOrderBy order = new SelectOrderBy();
        order.expression = expr;
        if (parseSortType) {
            order.sortType = parseSimpleSortType();
        }
        orderList.add(order);
        r.setOrderByList(orderList);
    }

    private ArrayList<SelectOrderBy> parseSimpleOrderList() {
        ArrayList<SelectOrderBy> orderList = Utils.newSmallArrayList();
        do {
            SelectOrderBy order = new SelectOrderBy();
            order.expression = readExpression();
            order.sortType = parseSortType();
            orderList.add(order);
        } while (readIf(COMMA));
        return orderList;
    }

    private JavaFunction readJavaFunction(Schema schema, String functionName, boolean throwIfNotFound) {
        FunctionAlias functionAlias;
        if (schema != null) {
            functionAlias = schema.findFunction(functionName);
        } else {
            functionAlias = findFunctionAlias(session.getCurrentSchemaName(),
                    functionName);
        }
        if (functionAlias == null) {
            if (throwIfNotFound) {
                throw DbException.get(ErrorCode.FUNCTION_NOT_FOUND_1, functionName);
            } else {
                return null;
            }
        }
        Expression[] args;
        ArrayList<Expression> argList = Utils.newSmallArrayList();
        if (!readIf(CLOSE_PAREN)) {
            do {
                argList.add(readExpression());
            } while (readIfMore(true));
        }
        args = argList.toArray(new Expression[0]);
        return new JavaFunction(functionAlias, args);
    }

    private JavaAggregate readJavaAggregate(UserAggregate aggregate) {
        boolean distinct = readDistinctAgg();
        ArrayList<Expression> params = Utils.newSmallArrayList();
        do {
            params.add(readExpression());
        } while (readIfMore(true));
        Expression[] list = params.toArray(new Expression[0]);
        JavaAggregate agg = new JavaAggregate(aggregate, list, currentSelect, distinct);
        readFilterAndOver(agg);
        return agg;
    }

    private boolean readDistinctAgg() {
        if (readIf(DISTINCT)) {
            return true;
        }
        readIf(ALL);
        return false;
    }

    private void readFilterAndOver(AbstractAggregate aggregate) {
        if (readIf("FILTER")) {
            read(OPEN_PAREN);
            read(WHERE);
            Expression filterCondition = readExpression();
            read(CLOSE_PAREN);
            aggregate.setFilterCondition(filterCondition);
        }
        readOver(aggregate);
    }

    private void readOver(DataAnalysisOperation operation) {
        if (readIf("OVER")) {
            operation.setOverCondition(readWindowNameOrSpecification());
            currentSelect.setWindowQuery();
        } else if (operation.isAggregate()) {
            currentSelect.setGroupQuery();
        } else {
            throw getSyntaxError();
        }
    }

    private Window readWindowNameOrSpecification() {
        return isToken(OPEN_PAREN) ? readWindowSpecification() : new Window(readAliasIdentifier(), null, null, null);
    }

    private Window readWindowSpecification() {
        read(OPEN_PAREN);
        String parent = null;
        if (currentTokenType == IDENTIFIER) {
            String token = currentToken;
            if (currentTokenQuoted || ( //
                    !equalsToken(token, "PARTITION") //
                    && !equalsToken(token, "ROWS") //
                    && !equalsToken(token, "RANGE") //
                    && !equalsToken(token, "GROUPS"))) {
                parent = token;
                read();
            }
        }
        ArrayList<Expression> partitionBy = null;
        if (readIf("PARTITION")) {
            read("BY");
            partitionBy = Utils.newSmallArrayList();
            do {
                Expression expr = readExpression();
                partitionBy.add(expr);
            } while (readIf(COMMA));
        }
        ArrayList<SelectOrderBy> orderBy = null;
        if (readIf(ORDER)) {
            read("BY");
            orderBy = parseSimpleOrderList();
        }
        WindowFrame frame = readWindowFrame();
        read(CLOSE_PAREN);
        return new Window(parent, partitionBy, orderBy, frame);
    }

    private WindowFrame readWindowFrame() {
        WindowFrameUnits units;
        if (readIf("ROWS")) {
            units = WindowFrameUnits.ROWS;
        } else if (readIf("RANGE")) {
            units = WindowFrameUnits.RANGE;
        } else if (readIf("GROUPS")) {
            units = WindowFrameUnits.GROUPS;
        } else {
            return null;
        }
        WindowFrameBound starting, following;
        if (readIf("BETWEEN")) {
            starting = readWindowFrameRange();
            read("AND");
            following = readWindowFrameRange();
        } else {
            starting = readWindowFrameStarting();
            following = null;
        }
        int idx = lastParseIndex;
        WindowFrameExclusion exclusion = WindowFrameExclusion.EXCLUDE_NO_OTHERS;
        if (readIf("EXCLUDE")) {
            if (readIf("CURRENT")) {
                read(ROW);
                exclusion = WindowFrameExclusion.EXCLUDE_CURRENT_ROW;
            } else if (readIf(GROUP)) {
                exclusion = WindowFrameExclusion.EXCLUDE_GROUP;
            } else if (readIf("TIES")) {
                exclusion = WindowFrameExclusion.EXCLUDE_TIES;
            } else {
                read("NO");
                read("OTHERS");
            }
        }
        WindowFrame frame = new WindowFrame(units, starting, following, exclusion);
        if (!frame.isValid()) {
            throw DbException.getSyntaxError(sqlCommand, idx);
        }
        return frame;
    }

    private WindowFrameBound readWindowFrameStarting() {
        if (readIf("UNBOUNDED")) {
            read("PRECEDING");
            return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_PRECEDING, null);
        }
        if (readIf("CURRENT")) {
            read(ROW);
            return new WindowFrameBound(WindowFrameBoundType.CURRENT_ROW, null);
        }
        Expression value = readExpression();
        read("PRECEDING");
        return new WindowFrameBound(WindowFrameBoundType.PRECEDING, value);
    }

    private WindowFrameBound readWindowFrameRange() {
        if (readIf("UNBOUNDED")) {
            if (readIf("PRECEDING")) {
                return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_PRECEDING, null);
            }
            read("FOLLOWING");
            return new WindowFrameBound(WindowFrameBoundType.UNBOUNDED_FOLLOWING, null);
        }
        if (readIf("CURRENT")) {
            read(ROW);
            return new WindowFrameBound(WindowFrameBoundType.CURRENT_ROW, null);
        }
        Expression value = readExpression();
        if (readIf("PRECEDING")) {
            return new WindowFrameBound(WindowFrameBoundType.PRECEDING, value);
        }
        read("FOLLOWING");
        return new WindowFrameBound(WindowFrameBoundType.FOLLOWING, value);
    }

    private AggregateType getAggregateType(String name) {
        if (!identifiersToUpper) {
            // if not yet converted to uppercase, do it now
            name = StringUtils.toUpperEnglish(name);
        }
        return Aggregate.getAggregateType(name);
    }

    private Expression readFunction(Schema schema, String name) {
        if (schema != null) {
            return readJavaFunction(schema, name, true);
        }
        boolean allowOverride = database.isAllowBuiltinAliasOverride();
        if (allowOverride) {
            JavaFunction jf = readJavaFunction(null, name, false);
            if (jf != null) {
                return jf;
            }
        }
        AggregateType agg = getAggregateType(name);
        if (agg != null) {
            return readAggregate(agg, name);
        }
        Function function = Function.getFunction(database, name);
        if (function == null) {
            WindowFunction windowFunction = readWindowFunction(name);
            if (windowFunction != null) {
                return windowFunction;
            }
            UserAggregate aggregate = database.findAggregate(name);
            if (aggregate != null) {
                return readJavaAggregate(aggregate);
            }
            if (allowOverride) {
                throw DbException.get(ErrorCode.FUNCTION_NOT_FOUND_1, name);
            }
            return readJavaFunction(null, name, true);
        }
        switch (function.getFunctionType()) {
        case Function.CAST: {
            function.setParameter(0, readExpression());
            read("AS");
            Column type = parseColumnWithType(null, false);
            function.setDataType(type);
            read(CLOSE_PAREN);
            break;
        }
        case Function.CONVERT: {
            if (database.getMode().swapConvertFunctionParameters) {
                Column type = parseColumnWithType(null, false);
                function.setDataType(type);
                read(COMMA);
                function.setParameter(0, readExpression());
                read(CLOSE_PAREN);
            } else {
                function.setParameter(0, readExpression());
                read(COMMA);
                Column type = parseColumnWithType(null, false);
                function.setDataType(type);
                read(CLOSE_PAREN);
            }
            break;
        }
        case Function.EXTRACT: {
            function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
            read();
            read(FROM);
            function.setParameter(1, readExpression());
            read(CLOSE_PAREN);
            break;
        }
        case Function.DATE_ADD:
        case Function.DATE_DIFF: {
            if (currentTokenType == VALUE) {
                function.setParameter(0, ValueExpression.get(currentValue.convertTo(Value.STRING)));
            } else {
                function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
            }
            read();
            read(COMMA);
            function.setParameter(1, readExpression());
            read(COMMA);
            function.setParameter(2, readExpression());
            read(CLOSE_PAREN);
            break;
        }
        case Function.SUBSTRING: {
            // Different variants include:
            // SUBSTRING(X,1)
            // SUBSTRING(X,1,1)
            // SUBSTRING(X FROM 1 FOR 1) -- Postgres
            // SUBSTRING(X FROM 1) -- Postgres
            // SUBSTRING(X FOR 1) -- Postgres
            function.setParameter(0, readExpression());
            if (readIf(FROM)) {
                function.setParameter(1, readExpression());
                if (readIf(FOR)) {
                    function.setParameter(2, readExpression());
                }
            } else if (readIf(FOR)) {
                function.setParameter(1, ValueExpression.get(ValueInt.get(0)));
                function.setParameter(2, readExpression());
            } else {
                read(COMMA);
                function.setParameter(1, readExpression());
                if (readIf(COMMA)) {
                    function.setParameter(2, readExpression());
                }
            }
            read(CLOSE_PAREN);
            break;
        }
        case Function.POSITION: {
            // can't read expression because IN would be read too early
            function.setParameter(0, readConcat());
            if (!readIf(COMMA)) {
                read("IN");
            }
            function.setParameter(1, readExpression());
            read(CLOSE_PAREN);
            break;
        }
        case Function.TRIM: {
            int flags;
            boolean needFrom = false;
            if (readIf("LEADING")) {
                flags = Function.TRIM_LEADING;
                needFrom = true;
            } else if (readIf("TRAILING")) {
                flags = Function.TRIM_TRAILING;
                needFrom = true;
            } else {
                needFrom = readIf("BOTH");
                flags = Function.TRIM_LEADING | Function.TRIM_TRAILING;
            }
            Expression p0, space = null;
            function.setFlags(flags);
            if (needFrom) {
                if (!readIf(FROM)) {
                    space = readExpression();
                    read(FROM);
                }
                p0 = readExpression();
            } else {
                if (readIf(FROM)) {
                    p0 = readExpression();
                } else {
                    p0 = readExpression();
                    if (readIf(FROM)) {
                        space = p0;
                        p0 = readExpression();
                    }
                }
            }
            if (!needFrom && space == null && readIf(COMMA)) {
                space = readExpression();
            }
            function.setParameter(0, p0);
            if (space != null) {
                function.setParameter(1, space);
            }
            read(CLOSE_PAREN);
            break;
        }
        case Function.TABLE:
        case Function.TABLE_DISTINCT: {
            int i = 0;
            ArrayList<Column> columns = Utils.newSmallArrayList();
            do {
                String columnName = readAliasIdentifier();
                Column column = parseColumnWithType(columnName, false);
                columns.add(column);
                read(EQUAL);
                function.setParameter(i, readExpression());
                i++;
            } while (readIfMore(true));
            TableFunction tf = (TableFunction) function;
            tf.setColumns(columns);
            break;
        }
        case Function.UNNEST: {
            ArrayList<Column> columns = Utils.newSmallArrayList();
            if (!readIf(CLOSE_PAREN)) {
                int i = 0;
                do {
                    function.setParameter(i++, readExpression());
                    columns.add(new Column("C" + i, Value.NULL));
                } while (readIfMore(true));
            }
            if (readIf(WITH)) {
                read("ORDINALITY");
                columns.add(new Column("NORD", Value.INT));
            }
            TableFunction tf = (TableFunction) function;
            tf.setColumns(columns);
            break;
        }
        default:
            if (!readIf(CLOSE_PAREN)) {
                int i = 0;
                do {
                    function.setParameter(i++, readExpression());
                } while (readIfMore(true));
            }
        }
        function.doneWithParameters();
        return function;
    }

    private WindowFunction readWindowFunction(String name) {
        if (!identifiersToUpper) {
            // if not yet converted to uppercase, do it now
            name = StringUtils.toUpperEnglish(name);
        }
        WindowFunctionType type = WindowFunctionType.get(name);
        if (type == null) {
            return null;
        }
        if (currentSelect == null) {
            throw getSyntaxError();
        }
        int numArgs = WindowFunction.getMinArgumentCount(type);
        Expression[] args = null;
        if (numArgs > 0) {
            // There is no functions with numArgs == 0 && numArgsMax > 0
            int numArgsMax = WindowFunction.getMaxArgumentCount(type);
            args = new Expression[numArgsMax];
            if (numArgs == numArgsMax) {
                for (int i = 0; i < numArgs; i++) {
                    if (i > 0) {
                        read(COMMA);
                    }
                    args[i] = readExpression();
                }
            } else {
                int i = 0;
                while (i < numArgsMax) {
                    if (i > 0 && !readIf(COMMA)) {
                        break;
                    }
                    args[i] = readExpression();
                    i++;
                }
                if (i < numArgs) {
                    throw getSyntaxError();
                }
                if (i != numArgsMax) {
                    args = Arrays.copyOf(args, i);
                }
            }
        }
        read(CLOSE_PAREN);
        WindowFunction function = new WindowFunction(type, currentSelect, args);
        if (type == WindowFunctionType.NTH_VALUE) {
            readFromFirstOrLast(function);
        }
        switch (type) {
        case LEAD:
        case LAG:
        case FIRST_VALUE:
        case LAST_VALUE:
        case NTH_VALUE:
            readRespectOrIgnoreNulls(function);
            //$FALL-THROUGH$
        default:
            // Avoid warning
        }
        readOver(function);
        return function;
    }

    private void readFromFirstOrLast(WindowFunction function) {
        if (readIf(FROM) && !readIf("FIRST")) {
            read("LAST");
            function.setFromLast(true);
        }
    }

    private void readRespectOrIgnoreNulls(WindowFunction function) {
        if (readIf("RESPECT")) {
            read("NULLS");
        } else if (readIf("IGNORE")) {
            read("NULLS");
            function.setIgnoreNulls(true);
        }
    }

    private Expression readKeywordFunction(String name) {
        if (readIf(OPEN_PAREN)) {
            return readFunction(null, name);
        } else {
            return readFunctionWithoutParameters(name);
        }
    }

    private Expression readFunctionWithoutParameters(String name) {
        if (database.isAllowBuiltinAliasOverride()) {
            FunctionAlias functionAlias = database.getSchema(session.getCurrentSchemaName()).findFunction(name);
            if (functionAlias != null) {
                return new JavaFunction(functionAlias, new Expression[0]);
            }
        }
        Function function = Function.getFunction(database, name);
        function.doneWithParameters();
        return function;
    }

    private Expression readWildcardRowidOrSequenceValue(String schema, String objectName) {
        if (readIf(ASTERISK)) {
            return parseWildcard(schema, objectName);
        }
        if (readIf(_ROWID_)) {
            return new ExpressionColumn(database, schema, objectName, Column.ROWID, true);
        }
        if (schema == null) {
            schema = session.getCurrentSchemaName();
        }
        if (readIf("NEXTVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                return new SequenceValue(sequence);
            }
        } else if (readIf("CURRVAL")) {
            Sequence sequence = findSequence(schema, objectName);
            if (sequence != null) {
                Function function = Function.getFunction(database, "CURRVAL");
                function.setParameter(0, ValueExpression.get(ValueString
                        .get(sequence.getSchema().getName())));
                function.setParameter(1, ValueExpression.get(ValueString
                        .get(sequence.getName())));
                function.doneWithParameters();
                return function;
            }
        }
        return null;
    }

    private Wildcard parseWildcard(String schema, String objectName) {
        Wildcard wildcard = new Wildcard(schema, objectName);
        if (readIf(EXCEPT)) {
            read(OPEN_PAREN);
            ArrayList<ExpressionColumn> exceptColumns = Utils.newSmallArrayList();
            do {
                String s = null, t = null;
                String name = readColumnIdentifier();
                if (readIf(DOT)) {
                    t = name;
                    name = readColumnIdentifier();
                    if (readIf(DOT)) {
                        s = t;
                        t = name;
                        name = readColumnIdentifier();
                        if (readIf(DOT)) {
                            if (!equalsToken(database.getShortName(), s)) {
                                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, s);
                            }
                            s = t;
                            t = name;
                            name = readColumnIdentifier();
                        }
                    }
                }
                exceptColumns.add(new ExpressionColumn(database, s, t, name, false));
            } while (readIfMore(true));
            wildcard.setExceptColumns(exceptColumns);
        }
        return wildcard;
    }

    private Expression readTermObjectDot(String objectName) {
        Expression expr = readWildcardRowidOrSequenceValue(null, objectName);
        if (expr != null) {
            return expr;
        }
        String name = readColumnIdentifier();
        Schema s = database.findSchema(objectName);
        if (readIf(OPEN_PAREN)) {
            return readFunction(s, name);
        } else if (readIf(DOT)) {
            String schema = objectName;
            objectName = name;
            expr = readWildcardRowidOrSequenceValue(schema, objectName);
            if (expr != null) {
                return expr;
            }
            name = readColumnIdentifier();
            if (readIf(OPEN_PAREN)) {
                String databaseName = schema;
                if (!equalsToken(database.getShortName(), databaseName)) {
                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1,
                            databaseName);
                }
                schema = objectName;
                return readFunction(database.getSchema(schema), name);
            } else if (readIf(DOT)) {
                String databaseName = schema;
                if (!equalsToken(database.getShortName(), databaseName)) {
                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1,
                            databaseName);
                }
                schema = objectName;
                objectName = name;
                expr = readWildcardRowidOrSequenceValue(schema, objectName);
                if (expr != null) {
                    return expr;
                }
                name = readColumnIdentifier();
                return new ExpressionColumn(database, schema, objectName, name, false);
            }
            return new ExpressionColumn(database, schema, objectName, name, false);
        }
        return new ExpressionColumn(database, null, objectName, name, false);
    }

    private Parameter readParameter() {
        // there must be no space between ? and the number
        boolean indexed = Character.isDigit(sqlCommandChars[parseIndex]);

        Parameter p;
        if (indexed) {
            readParameterIndex();
            if (indexedParameterList == null) {
                if (parameters == null) {
                    // this can occur when parsing expressions only (for
                    // example check constraints)
                    throw getSyntaxError();
                } else if (!parameters.isEmpty()) {
                    throw DbException
                            .get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
                }
                indexedParameterList = Utils.newSmallArrayList();
            }
            int index = currentValue.getInt() - 1;
            if (index < 0 || index >= Constants.MAX_PARAMETER_INDEX) {
                throw DbException.getInvalidValueException(
                        "parameter index", index + 1);
            }
            if (indexedParameterList.size() <= index) {
                indexedParameterList.ensureCapacity(index + 1);
                while (indexedParameterList.size() <= index) {
                    indexedParameterList.add(null);
                }
            }
            p = indexedParameterList.get(index);
            if (p == null) {
                p = new Parameter(index);
                indexedParameterList.set(index, p);
                parameters.add(p);
            }
            read();
        } else {
            read();
            if (indexedParameterList != null) {
                throw DbException
                        .get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
            }
            p = new Parameter(parameters.size());
            parameters.add(p);
        }
        return p;
    }

    private Expression readTerm() {
        Expression r;
        switch (currentTokenType) {
        case AT:
            read();
            r = new Variable(session, readAliasIdentifier());
            if (readIf(COLON_EQ)) {
                Expression value = readExpression();
                Function function = Function.getFunction(database, "SET");
                function.setParameter(0, r);
                function.setParameter(1, value);
                r = function;
            }
            break;
        case PARAMETER:
            r = readParameter();
            break;
        case SELECT:
        case FROM:
        case WITH:
            r = new Subquery(parseSelect());
            break;
        case TABLE:
            int index = lastParseIndex;
            read();
            if (readIf(OPEN_PAREN)) {
                r = readFunction(null, "TABLE");
            } else {
                parseIndex = index;
                read();
                r = new Subquery(parseSelect());
            }
            break;
        case IDENTIFIER:
            String name = currentToken;
            if (currentTokenQuoted) {
                read();
                if (readIf(OPEN_PAREN)) {
                    r = readFunction(null, name);
                } else if (readIf(DOT)) {
                    r = readTermObjectDot(name);
                } else {
                    r = new ExpressionColumn(database, null, null, name, false);
                }
            } else {
                read();
                if (readIf(DOT)) {
                    r = readTermObjectDot(name);
                } else if (readIf(OPEN_PAREN)) {
                    r = readFunction(null, name);
                } else {
                    r = readTermWithIdentifier(name);
                }
            }
            break;
        case MINUS_SIGN:
            read();
            if (currentTokenType == VALUE) {
                r = ValueExpression.get(currentValue.negate());
                int rType = r.getType().getValueType();
                if (rType == Value.LONG &&
                        r.getValue(session).getLong() == Integer.MIN_VALUE) {
                    // convert Integer.MIN_VALUE to type 'int'
                    // (Integer.MAX_VALUE+1 is of type 'long')
                    r = ValueExpression.get(ValueInt.get(Integer.MIN_VALUE));
                } else if (rType == Value.DECIMAL &&
                        r.getValue(session).getBigDecimal().compareTo(Value.MIN_LONG_DECIMAL) == 0) {
                    // convert Long.MIN_VALUE to type 'long'
                    // (Long.MAX_VALUE+1 is of type 'decimal')
                    r = ValueExpression.get(ValueLong.MIN);
                }
                read();
            } else {
                r = new UnaryOperation(readTerm());
            }
            break;
        case PLUS_SIGN:
            read();
            r = readTerm();
            break;
        case OPEN_PAREN:
            read();
            if (readIf(CLOSE_PAREN)) {
                r = ValueExpression.get(ValueRow.getEmpty());
            } else {
                r = readExpression();
                if (readIfMore(true)) {
                    ArrayList<Expression> list = Utils.newSmallArrayList();
                    list.add(r);
                    if (!readIf(CLOSE_PAREN)) {
                        do {
                            list.add(readExpression());
                        } while (readIfMore(false));
                    }
                    r = new ExpressionList(list.toArray(new Expression[0]), false);
                }
            }
            break;
        case ARRAY:
            read();
            read(OPEN_BRACKET);
            if (readIf(CLOSE_BRACKET)) {
                r = ValueExpression.get(ValueArray.getEmpty());
            } else {
                ArrayList<Expression> list = Utils.newSmallArrayList();
                list.add(readExpression());
                while (readIf(COMMA)) {
                    list.add(readExpression());
                }
                read(CLOSE_BRACKET);
                r = new ExpressionList(list.toArray(new Expression[0]), true);
            }
            break;
        case INTERVAL:
            read();
            r = readInterval();
            break;
        case ROW: {
            read();
            read(OPEN_PAREN);
            if (readIf(CLOSE_PAREN)) {
                r = ValueExpression.get(ValueRow.getEmpty());
            } else {
                ArrayList<Expression> list = Utils.newSmallArrayList();
                do {
                    list.add(readExpression());
                } while (readIfMore(true));
                r = new ExpressionList(list.toArray(new Expression[0]), false);
            }
            break;
        }
        case TRUE:
            read();
            r = ValueExpression.get(ValueBoolean.TRUE);
            break;
        case FALSE:
            read();
            r = ValueExpression.get(ValueBoolean.FALSE);
            break;
        case ROWNUM:
            read();
            if (readIf(OPEN_PAREN)) {
                read(CLOSE_PAREN);
            }
            if (currentSelect == null && currentPrepared == null) {
                throw getSyntaxError();
            }
            r = new Rownum(currentSelect == null ? currentPrepared
                    : currentSelect);
            break;
        case NULL:
            read();
            r = ValueExpression.getNull();
            break;
        case _ROWID_:
            read();
            r = new ExpressionColumn(database, null, null, Column.ROWID, true);
            break;
        case VALUE:
            r = ValueExpression.get(currentValue);
            read();
            break;
        case VALUES:
            if (database.getMode().onDuplicateKeyUpdate) {
                read();
                r = readKeywordFunction("VALUES");
            } else {
                r = new Subquery(parseSelect());
            }
            break;
        case CASE:
            read();
            r = readCase();
            break;
        case CURRENT_DATE:
            read();
            r = readKeywordFunction("CURRENT_DATE");
            break;
        case CURRENT_TIME:
            read();
            r = readKeywordFunction("CURRENT_TIME");
            break;
        case CURRENT_TIMESTAMP:
            read();
            r = readKeywordFunction("CURRENT_TIMESTAMP");
            break;
        case CURRENT_USER:
            read();
            r = readKeywordFunction("USER");
            break;
        case LOCALTIME:
            read();
            r = readKeywordFunction("LOCALTIME");
            break;
        case LOCALTIMESTAMP:
            read();
            r = readKeywordFunction("LOCALTIMESTAMP");
            break;
        default:
            throw getSyntaxError();
        }
        if (readIf(OPEN_BRACKET)) {
            Function function = Function.getFunction(database, "ARRAY_GET");
            function.setParameter(0, r);
            function.setParameter(1, readExpression());
            r = function;
            read(CLOSE_BRACKET);
        }
        if (readIf(COLON_COLON)) {
            // PostgreSQL compatibility
            if (isToken("PG_CATALOG")) {
                read("PG_CATALOG");
                read(DOT);
            }
            if (readIf("REGCLASS")) {
                FunctionAlias f = findFunctionAlias(database.getMainSchema().getName(), "PG_GET_OID");
                if (f == null) {
                    throw getSyntaxError();
                }
                Expression[] args = { r };
                r = new JavaFunction(f, args);
            } else {
                Column col = parseColumnWithType(null, false);
                Function function = Function.getFunction(database, "CAST");
                function.setDataType(col);
                function.setParameter(0, r);
                r = function;
            }
        }
        return r;
    }

    private Expression readTermWithIdentifier(String name) {
        // Unquoted identifier is never empty
        char ch = name.charAt(0);
        if (!identifiersToUpper) {
            /*
             * Convert a-z to A-Z. This method is safe, because only A-Z
             * characters are considered below.
             */
            ch &= 0xffdf;
        }
        switch (ch) {
        case 'C':
            if (database.getMode().getEnum() == ModeEnum.DB2 && equalsToken("CURRENT", name)) {
                return parseDB2SpecialRegisters(name);
            }
            break;
        case 'D':
            if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING &&
                    (equalsToken("DATE", name) || equalsToken("D", name))) {
                String date = currentValue.getString();
                read();
                return ValueExpression.get(ValueDate.parse(date));
            }
            break;
        case 'E':
            if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING && equalsToken("E", name)) {
                String text = currentValue.getString();
                // the PostgreSQL ODBC driver uses
                // LIKE E'PROJECT\\_DATA' instead of LIKE
                // 'PROJECT\_DATA'
                // N: SQL-92 "National Language" strings
                text = StringUtils.replaceAll(text, "\\\\", "\\");
                read();
                return ValueExpression.get(ValueString.get(text));
            }
            break;
        case 'N':
            if (equalsToken("NEXT", name) && readIf("VALUE")) {
                read(FOR);
                return new SequenceValue(readSequence());
            } else if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING
                    && equalsToken("N", name)) {
                // SQL-92 "National Language" strings
                String text = currentValue.getString();
                read();
                return ValueExpression.get(ValueString.get(text));
            }
            break;
        case 'S':
            if (equalsToken("SYSDATE", name)) {
                return readFunctionWithoutParameters("CURRENT_TIMESTAMP");
            } else if (equalsToken("SYSTIME", name)) {
                return readFunctionWithoutParameters("CURRENT_TIME");
            } else if (equalsToken("SYSTIMESTAMP", name)) {
                return readFunctionWithoutParameters("CURRENT_TIMESTAMP");
            }
            break;
        case 'T':
            if (equalsToken("TIME", name)) {
                boolean without = readIf("WITHOUT");
                if (without) {
                    read("TIME");
                    read("ZONE");
                }
                if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING) {
                    String time = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTime.parse(time));
                } else if (without) {
                    throw getSyntaxError();
                }
            } else if (equalsToken("TIMESTAMP", name)) {
                if (readIf(WITH)) {
                    read("TIME");
                    read("ZONE");
                    if (currentTokenType != VALUE || currentValue.getValueType() != Value.STRING) {
                        throw getSyntaxError();
                    }
                    String timestamp = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTimestampTimeZone.parse(timestamp));
                } else {
                    boolean without = readIf("WITHOUT");
                    if (without) {
                        read("TIME");
                        read("ZONE");
                    }
                    if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING) {
                        String timestamp = currentValue.getString();
                        read();
                        return ValueExpression.get(ValueTimestamp.parse(timestamp, database.getMode()));
                    } else if (without) {
                        throw getSyntaxError();
                    }
                }
            } else if (equalsToken("TODAY", name)) {
                return readFunctionWithoutParameters("CURRENT_DATE");
            } else if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING) {
                if (equalsToken("T", name)) {
                    String time = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTime.parse(time));
                } else if (equalsToken("TS", name)) {
                    String timestamp = currentValue.getString();
                    read();
                    return ValueExpression.get(ValueTimestamp.parse(timestamp, database.getMode()));
                }
            }
            break;
        case 'X':
            if (currentTokenType == VALUE && currentValue.getValueType() == Value.STRING && equalsToken("X", name)) {
                byte[] buffer = StringUtils.convertHexToBytes(currentValue.getString());
                read();
                return ValueExpression.get(ValueBytes.getNoCopy(buffer));
            }
            break;
        }
        return new ExpressionColumn(database, null, null, name, false);
    }

    private Expression readInterval() {
        boolean negative = readIf(MINUS_SIGN);
        if (!negative) {
            readIf(PLUS_SIGN);
        }
        String s = readString();
        IntervalQualifier qualifier;
        if (readIf("YEAR")) {
            if (readIf("TO")) {
                read("MONTH");
                qualifier = IntervalQualifier.YEAR_TO_MONTH;
            } else {
                qualifier = IntervalQualifier.YEAR;
            }
        } else if (readIf("MONTH")) {
            qualifier = IntervalQualifier.MONTH;
        } else if (readIf("DAY")) {
            if (readIf("TO")) {
                if (readIf("HOUR")) {
                    qualifier = IntervalQualifier.DAY_TO_HOUR;
                } else if (readIf("MINUTE")) {
                    qualifier = IntervalQualifier.DAY_TO_MINUTE;
                } else {
                    read("SECOND");
                    qualifier = IntervalQualifier.DAY_TO_SECOND;
                }
            } else {
                qualifier = IntervalQualifier.DAY;
            }
        } else if (readIf("HOUR")) {
            if (readIf("TO")) {
                if (readIf("MINUTE")) {
                    qualifier = IntervalQualifier.HOUR_TO_MINUTE;
                } else {
                    read("SECOND");
                    qualifier = IntervalQualifier.HOUR_TO_SECOND;
                }
            } else {
                qualifier = IntervalQualifier.HOUR;
            }
        } else if (readIf("MINUTE")) {
            if (readIf("TO")) {
                read("SECOND");
                qualifier = IntervalQualifier.MINUTE_TO_SECOND;
            } else {
                qualifier = IntervalQualifier.MINUTE;
            }
        } else {
            read("SECOND");
            qualifier = IntervalQualifier.SECOND;
        }
        try {
            return ValueExpression.get(IntervalUtils.parseInterval(qualifier, negative, s));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
        }
    }

    private Expression parseDB2SpecialRegisters(String name) {
        // Only "CURRENT" name is supported
        if (readIf("TIMESTAMP")) {
            if (readIf(WITH)) {
                read("TIME");
                read("ZONE");
                return readKeywordFunction("CURRENT_TIMESTAMP");
            }
            return readKeywordFunction("LOCALTIMESTAMP");
        } else if (readIf("TIME")) {
            // Time with fractional seconds is not supported by DB2
            return readFunctionWithoutParameters("CURRENT_TIME");
        } else if (readIf("DATE")) {
            return readFunctionWithoutParameters("CURRENT_DATE");
        }
        // No match, parse CURRENT as a column
        return new ExpressionColumn(database, null, null, name, false);
    }

    private Expression readCase() {
        if (readIf("END")) {
            readIf(CASE);
            return ValueExpression.getNull();
        }
        if (readIf("ELSE")) {
            Expression elsePart = readExpression().optimize(session);
            read("END");
            readIf(CASE);
            return elsePart;
        }
        int i;
        Function function;
        if (readIf("WHEN")) {
            function = Function.getFunction(database, "CASE");
            function.setParameter(0, null);
            i = 1;
            do {
                function.setParameter(i++, readExpression());
                read("THEN");
                function.setParameter(i++, readExpression());
            } while (readIf("WHEN"));
        } else {
            Expression expr = readExpression();
            if (readIf("END")) {
                readIf(CASE);
                return ValueExpression.getNull();
            }
            if (readIf("ELSE")) {
                Expression elsePart = readExpression().optimize(session);
                read("END");
                readIf(CASE);
                return elsePart;
            }
            function = Function.getFunction(database, "CASE");
            function.setParameter(0, expr);
            i = 1;
            read("WHEN");
            do {
                function.setParameter(i++, readExpression());
                read("THEN");
                function.setParameter(i++, readExpression());
            } while (readIf("WHEN"));
        }
        if (readIf("ELSE")) {
            function.setParameter(i, readExpression());
        }
        read("END");
        readIf("CASE");
        function.doneWithParameters();
        return function;
    }

    private int readNonNegativeInt() {
        int v = readInt();
        if (v < 0) {
            throw DbException.getInvalidValueException("non-negative integer", v);
        }
        return v;
    }

    private int readInt() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != VALUE) {
            throw DbException.getSyntaxError(sqlCommand, parseIndex, "integer");
        }
        if (minus) {
            // must do that now, otherwise Integer.MIN_VALUE would not work
            currentValue = currentValue.negate();
        }
        int i = currentValue.getInt();
        read();
        return i;
    }

    private long readNonNegativeLong() {
        long v = readLong();
        if (v < 0) {
            throw DbException.getInvalidValueException("non-negative long", v);
        }
        return v;
    }

    private long readLong() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != VALUE) {
            throw DbException.getSyntaxError(sqlCommand, parseIndex, "long");
        }
        if (minus) {
            // must do that now, otherwise Long.MIN_VALUE would not work
            currentValue = currentValue.negate();
        }
        long i = currentValue.getLong();
        read();
        return i;
    }

    private boolean readBooleanSetting() {
        switch (currentTokenType) {
        case ON:
        case TRUE:
            read();
            return true;
        case FALSE:
            read();
            return false;
        case VALUE:
            boolean result = currentValue.getBoolean();
            read();
            return result;
        }
        if (readIf("OFF")) {
            return false;
        } else {
            throw getSyntaxError();
        }
    }

    private String readString() {
        Expression expr = readExpression().optimize(session);
        if (!(expr instanceof ValueExpression)) {
            throw DbException.getSyntaxError(sqlCommand, parseIndex, "string");
        }
        return expr.getValue(session).getString();
    }

    // TODO: why does this function allow defaultSchemaName=null - which resets
    // the parser schemaName for everyone ?
    private String readIdentifierWithSchema(String defaultSchemaName) {
        String s = readColumnIdentifier();
        schemaName = defaultSchemaName;
        if (readIf(DOT)) {
            schemaName = s;
            s = readColumnIdentifier();
        }
        if (currentTokenType == DOT) {
            if (equalsToken(schemaName, database.getShortName())) {
                read();
                schemaName = s;
                s = readColumnIdentifier();
            }
        }
        return s;
    }

    private String readIdentifierWithSchema() {
        return readIdentifierWithSchema(session.getCurrentSchemaName());
    }

    private String readAliasIdentifier() {
        return readColumnIdentifier();
    }

    private String readUniqueIdentifier() {
        return readColumnIdentifier();
    }

    private String readColumnIdentifier() {
        if (currentTokenType != IDENTIFIER) {
            /*
             * Sometimes a new keywords are introduced. During metadata
             * initialization phase keywords are accepted as identifiers to
             * allow migration from older versions.
             *
             * PageStore's LobStorageBackend also needs this in databases that
             * were created in 1.4.197 and older versions.
             */
            if (!session.getDatabase().isStarting() || !isKeyword(currentToken)) {
                throw DbException.getSyntaxError(sqlCommand, parseIndex, "identifier");
            }
        }
        String s = currentToken;
        read();
        return s;
    }

    private void read(String expected) {
        if (currentTokenQuoted || !equalsToken(expected, currentToken)) {
            addExpected(expected);
            throw getSyntaxError();
        }
        read();
    }

    private void read(int tokenType) {
        if (tokenType != currentTokenType) {
            addExpected(tokenType);
            throw getSyntaxError();
        }
        read();
    }

    private boolean readIf(String token) {
        if (!currentTokenQuoted && equalsToken(token, currentToken)) {
            read();
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean readIf(int tokenType) {
        if (tokenType == currentTokenType) {
            read();
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    private boolean isToken(String token) {
        if (!currentTokenQuoted && equalsToken(token, currentToken)) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean isToken(int tokenType) {
        if (tokenType == currentTokenType) {
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    private boolean equalsToken(String a, String b) {
        if (a == null) {
            return b == null;
        } else
            return a.equals(b) || !identifiersToUpper && a.equalsIgnoreCase(b);
    }

    private static boolean equalsTokenIgnoreCase(String a, String b) {
        if (a == null) {
            return b == null;
        } else
            return a.equals(b) || a.equalsIgnoreCase(b);
    }

    private boolean isTokenInList(Collection<String> upperCaseTokenList) {
        String upperCaseCurrentToken = currentToken.toUpperCase();
        return upperCaseTokenList.contains(upperCaseCurrentToken);
    }

    private void addExpected(String token) {
        if (expectedList != null) {
            expectedList.add(token);
        }
    }

    private void addExpected(int tokenType) {
        if (expectedList != null) {
            expectedList.add(TOKENS[tokenType]);
        }
    }

    private void read() {
        currentTokenQuoted = false;
        if (expectedList != null) {
            expectedList.clear();
        }
        int[] types = characterTypes;
        lastParseIndex = parseIndex;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {
            type = types[++i];
        }
        int start = i;
        char[] chars = sqlCommandChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
        case CHAR_NAME:
            while (true) {
                type = types[i];
                if (type != CHAR_NAME && type != CHAR_VALUE) {
                    break;
                }
                i++;
            }
            currentTokenType = ParserUtil.getSaveTokenType(sqlCommand, !identifiersToUpper, start, i, false);
            if (currentTokenType == IDENTIFIER) {
                currentToken = StringUtils.cache(sqlCommand.substring(start, i));
            } else {
                currentToken = TOKENS[currentTokenType];
            }
            parseIndex = i;
            return;
        case CHAR_QUOTED: {
            String result = null;
            while (true) {
                for (int begin = i;; i++) {
                    if (chars[i] == c) {
                        if (result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin - 1, i);
                        }
                        break;
                    }
                }
                if (chars[++i] != c) {
                    break;
                }
                i++;
            }
            currentToken = StringUtils.cache(result);
            parseIndex = i;
            currentTokenQuoted = true;
            currentTokenType = IDENTIFIER;
            return;
        }
        case CHAR_SPECIAL_2:
            if (types[i] == CHAR_SPECIAL_2) {
                char c1 = chars[i++];
                currentTokenType = getSpecialType2(c, c1);
            } else {
                currentTokenType = getSpecialType1(c);
            }
            parseIndex = i;
            return;
        case CHAR_SPECIAL_1:
            currentTokenType = getSpecialType1(c);
            parseIndex = i;
            return;
        case CHAR_VALUE:
            if (c == '0' && (chars[i] == 'X' || chars[i] == 'x')) {
                // hex number
                long number = 0;
                start += 2;
                i++;
                while (true) {
                    c = chars[i];
                    if (c >= '0' && c <= '9') {
                        number = (number << 4) + c - '0';
                    } else if (c >= 'A' && c <= 'F') {
                        number = (number << 4) + c - ('A' - 10);
                    } else if (c >= 'a' && c <= 'f') {
                        number = (number << 4) + c - ('a' - 10);
                    } else {
                        checkLiterals(false);
                        currentValue = ValueInt.get((int) number);
                        currentTokenType = VALUE;
                        currentToken = "0";
                        parseIndex = i;
                        return;
                    }
                    if (number > Integer.MAX_VALUE) {
                        readHexDecimal(start, i);
                        return;
                    }
                    i++;
                }
            }
            long number = c - '0';
            loop: while (true) {
                c = chars[i];
                if (c < '0' || c > '9') {
                    switch (c) {
                    case '.':
                    case 'E':
                    case 'e':
                        readDecimal(start, i, false);
                        break loop;
                    case 'L':
                    case 'l':
                        readDecimal(start, i, true);
                        break loop;
                    }
                    checkLiterals(false);
                    currentValue = ValueInt.get((int) number);
                    currentTokenType = VALUE;
                    currentToken = "0";
                    parseIndex = i;
                    break;
                }
                number = number * 10 + (c - '0');
                if (number > Integer.MAX_VALUE) {
                    readDecimal(start, i, true);
                    break;
                }
                i++;
            }
            return;
        case CHAR_DOT:
            if (types[i] != CHAR_VALUE) {
                currentTokenType = DOT;
                currentToken = ".";
                parseIndex = i;
                return;
            }
            readDecimal(i - 1, i, false);
            return;
        case CHAR_STRING: {
            String result = null;
            while (true) {
                for (int begin = i;; i++) {
                    if (chars[i] == '\'') {
                        if (result == null) {
                            result = sqlCommand.substring(begin, i);
                        } else {
                            result += sqlCommand.substring(begin - 1, i);
                        }
                        break;
                    }
                }
                if (chars[++i] != '\'') {
                    break;
                }
                i++;
            }
            currentToken = "'";
            checkLiterals(true);
            currentValue = ValueString.get(result, database.getMode().treatEmptyStringsAsNull);
            parseIndex = i;
            currentTokenType = VALUE;
            return;
        }
        case CHAR_DOLLAR_QUOTED_STRING: {
            int begin = i - 1;
            while (types[i] == CHAR_DOLLAR_QUOTED_STRING) {
                i++;
            }
            String result = sqlCommand.substring(begin, i);
            currentToken = "'";
            checkLiterals(true);
            currentValue = ValueString.get(result, database.getMode().treatEmptyStringsAsNull);
            parseIndex = i;
            currentTokenType = VALUE;
            return;
        }
        case CHAR_END:
            currentTokenType = END;
            parseIndex = i;
            return;
        default:
            throw getSyntaxError();
        }
    }

    private void readParameterIndex() {
        int i = parseIndex;

        char[] chars = sqlCommandChars;
        char c = chars[i++];
        long number = c - '0';
        while (true) {
            c = chars[i];
            if (c < '0' || c > '9') {
                currentValue = ValueInt.get((int) number);
                currentTokenType = VALUE;
                currentToken = "0";
                parseIndex = i;
                break;
            }
            number = number * 10 + (c - '0');
            if (number > Integer.MAX_VALUE) {
                throw DbException.getInvalidValueException(
                        "parameter index", number);
            }
            i++;
        }
    }

    private void checkLiterals(boolean text) {
        if (!literalsChecked && !session.getAllowLiterals()) {
            int allowed = database.getAllowLiterals();
            if (allowed == Constants.ALLOW_LITERALS_NONE ||
                    (text && allowed != Constants.ALLOW_LITERALS_ALL)) {
                throw DbException.get(ErrorCode.LITERALS_ARE_NOT_ALLOWED);
            }
        }
    }

    private void readHexDecimal(int start, int i) {
        char[] chars = sqlCommandChars;
        char c;
        do {
            c = chars[++i];
        } while ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'));
        parseIndex = i;
        String sub = sqlCommand.substring(start, i);
        BigDecimal bd = new BigDecimal(new BigInteger(sub, 16));
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    private void readDecimal(int start, int i, boolean integer) {
        char[] chars = sqlCommandChars;
        int[] types = characterTypes;
        // go until the first non-number
        while (true) {
            int t = types[i];
            if (t == CHAR_DOT) {
                integer = false;
            } else if (t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        char c = chars[i];
        if (c == 'E' || c == 'e') {
            integer = false;
            c = chars[++i];
            if (c == '+' || c == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) {
                throw getSyntaxError();
            }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        checkLiterals(false);
        BigDecimal bd;
        if (integer && i - start <= 19) {
            BigInteger bi = new BigInteger(sqlCommand.substring(start, i));
            if (bi.compareTo(ValueLong.MAX_BI) <= 0) {
                // parse constants like "10000000L"
                c = chars[i];
                if (c == 'L' || c == 'l') {
                    parseIndex++;
                }
                currentValue = ValueLong.get(bi.longValue());
                currentTokenType = VALUE;
                return;
            }
            bd = new BigDecimal(bi);
        } else {
            try {
                bd = new BigDecimal(sqlCommandChars, start, i - start);
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, sqlCommand.substring(start, i));
            }
        }
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    private void initialize(String sql) {
        if (sql == null) {
            sql = "";
        }
        originalSQL = sql;
        sqlCommand = sql;
        int len = sql.length() + 1;
        char[] command = new char[len];
        int[] types = new int[len];
        len--;
        sql.getChars(0, len, command, 0);
        boolean changed = false;
        command[len] = ' ';
        int startLoop = 0;
        int lastType = 0;
        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
            case '/':
                if (command[i + 1] == '*') {
                    // block comment
                    changed = true;
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    startLoop = i;
                    i += 2;
                    checkRunOver(i, len, startLoop);
                    while (command[i] != '*' || command[i + 1] != '/') {
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    i++;
                } else if (command[i + 1] == '/') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len - 1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '-':
                if (command[i + 1] == '-') {
                    // single line comment
                    changed = true;
                    startLoop = i;
                    while (true) {
                        c = command[i];
                        if (c == '\n' || c == '\r' || i >= len - 1) {
                            break;
                        }
                        command[i++] = ' ';
                        checkRunOver(i, len, startLoop);
                    }
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '$':
                if (command[i + 1] == '$' && (i == 0 || command[i - 1] <= ' ')) {
                    // dollar quoted string
                    changed = true;
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    startLoop = i;
                    i += 2;
                    checkRunOver(i, len, startLoop);
                    while (command[i] != '$' || command[i + 1] != '$') {
                        types[i++] = CHAR_DOLLAR_QUOTED_STRING;
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = ' ';
                    command[i + 1] = ' ';
                    i++;
                } else {
                    if (lastType == CHAR_NAME || lastType == CHAR_VALUE) {
                        // $ inside an identifier is supported
                        type = CHAR_NAME;
                    } else {
                        // but not at the start, to support PostgreSQL $1
                        type = CHAR_SPECIAL_1;
                    }
                }
                break;
            case '(':
            case ')':
            case '{':
            case '}':
            case '*':
            case ',':
            case ';':
            case '+':
            case '%':
            case '?':
            case '@':
            case ']':
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '|':
            case '=':
            case ':':
            case '&':
            case '~':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DOT;
                break;
            case '\'':
                type = types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '[':
                if (database.getMode().squareBracketQuotedNames) {
                    // SQL Server alias for "
                    command[i] = '"';
                    changed = true;
                    type = types[i] = CHAR_QUOTED;
                    startLoop = i;
                    while (command[++i] != ']') {
                        checkRunOver(i, len, startLoop);
                    }
                    command[i] = '"';
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            case '`':
                // MySQL alias for ", but not case sensitive
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '`') {
                    checkRunOver(i, len, startLoop);
                    c = command[i];
                    if (identifiersToUpper || identifiersToLower) {
                        char u = identifiersToUpper ? Character.toUpperCase(c) : Character.toLowerCase(c);
                        if (u != c) {
                            command[i] = u;
                            changed = true;
                        }
                    }
                }
                break;
            case '"':
                type = types[i] = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '_':
                type = CHAR_NAME;
                break;
            case '#':
                if (database.getMode().supportPoundSymbolForColumnNames) {
                    type = CHAR_NAME;
                } else {
                    type = CHAR_SPECIAL_1;
                }
                break;
            default:
                if (c >= 'a' && c <= 'z') {
                    if (identifiersToUpper) {
                        command[i] = (char) (c - ('a' - 'A'));
                        changed = true;
                    }
                    type = CHAR_NAME;
                } else if (c >= 'A' && c <= 'Z') {
                    if (identifiersToLower) {
                        command[i] = (char) (c + ('a' - 'A'));
                        changed = true;
                    }
                    type = CHAR_NAME;
                } else if (c >= '0' && c <= '9') {
                    type = CHAR_VALUE;
                } else {
                    if (c <= ' ' || Character.isSpaceChar(c)) {
                        // whitespace
                    } else if (Character.isJavaIdentifierPart(c)) {
                        type = CHAR_NAME;
                        if (identifiersToUpper || identifiersToLower) {
                            char u = identifiersToUpper ? Character.toUpperCase(c) : Character.toLowerCase(c);
                            if (u != c) {
                                command[i] = u;
                                changed = true;
                            }
                        }
                    } else {
                        type = CHAR_SPECIAL_1;
                    }
                }
            }
            types[i] = type;
            lastType = type;
        }
        sqlCommandChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        if (changed) {
            sqlCommand = new String(command);
        }
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) {
        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private int getSpecialType1(char c0) {
        switch (c0) {
        case '?':
        case '$':
            return PARAMETER;
        case '@':
            return AT;
        case '+':
            return PLUS_SIGN;
        case '-':
            return MINUS_SIGN;
        case '*':
            return ASTERISK;
        case ',':
            return COMMA;
        case '{':
            return OPEN_BRACE;
        case '}':
            return CLOSE_BRACE;
        case '/':
            return SLASH;
        case '%':
            return PERCENT;
        case ';':
            return SEMICOLON;
        case ':':
            return COLON;
        case '[':
            return OPEN_BRACKET;
        case ']':
            return CLOSE_BRACKET;
        case '~':
            return TILDE;
        case '(':
            return OPEN_PAREN;
        case ')':
            return CLOSE_PAREN;
        case '<':
            return SMALLER;
        case '>':
            return BIGGER;
        case '=':
            return EQUAL;
        default:
            throw getSyntaxError();
        }
    }

    private int getSpecialType2(char c0, char c1) {
        switch (c0) {
        case ':':
            if (c1 == ':') {
                return COLON_COLON;
            } else if (c1 == '=') {
                return COLON_EQ;
            }
            break;
        case '>':
            if (c1 == '=') {
                return BIGGER_EQUAL;
            }
            break;
        case '<':
            if (c1 == '=') {
                return SMALLER_EQUAL;
            } else if (c1 == '>') {
                return NOT_EQUAL;
            }
            break;
        case '!':
            if (c1 == '=') {
                return NOT_EQUAL;
            } else if (c1 == '~') {
                return NOT_TILDE;
            }
            break;
        case '|':
            if (c1 == '|') {
                return STRING_CONCAT;
            }
            break;
        case '&':
            if (c1 == '&') {
                return SPATIAL_INTERSECTS;
            }
            break;
        }
        throw getSyntaxError();
    }

    private boolean isKeyword(String s) {
        return ParserUtil.isKeyword(s, !identifiersToUpper);
    }

    private Column parseColumnForTable(String columnName,
            boolean defaultNullable, boolean forTable) {
        Column column;
        boolean isIdentity = readIf("IDENTITY");
        if (isIdentity || readIf("BIGSERIAL")) {
            // Check if any of them are disallowed in the current Mode
            if (isIdentity && database.getMode().
                    disallowedTypes.contains("IDENTITY")) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                        currentToken);
            }
            column = new Column(columnName, Value.LONG);
            column.setOriginalSQL("IDENTITY");
            parseAutoIncrement(column);
            // PostgreSQL compatibility
            if (!database.getMode().serialColumnIsNotPK) {
                column.setPrimaryKey(true);
            }
        } else if (readIf("SERIAL")) {
            column = new Column(columnName, Value.INT);
            column.setOriginalSQL("SERIAL");
            parseAutoIncrement(column);
            // PostgreSQL compatibility
            if (!database.getMode().serialColumnIsNotPK) {
                column.setPrimaryKey(true);
            }
        } else {
            column = parseColumnWithType(columnName, forTable);
        }
        if (readIf("INVISIBLE")) {
            column.setVisible(false);
        } else if (readIf("VISIBLE")) {
            column.setVisible(true);
        }
        NullConstraintType nullConstraint = parseNotNullConstraint();
        switch (nullConstraint) {
        case NULL_IS_ALLOWED:
            column.setNullable(true);
            break;
        case NULL_IS_NOT_ALLOWED:
            column.setNullable(false);
            break;
        case NO_NULL_CONSTRAINT_FOUND:
            // domains may be defined as not nullable
            column.setNullable(defaultNullable & column.isNullable());
            break;
        default:
            throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                    "Internal Error - unhandled case: " + nullConstraint.name());
        }
        if (readIf("AS")) {
            if (isIdentity) {
                getSyntaxError();
            }
            Expression expr = readExpression();
            column.setComputedExpression(expr);
        } else if (readIf("DEFAULT")) {
            Expression defaultExpression = readExpression();
            column.setDefaultExpression(session, defaultExpression);
        } else if (readIf("GENERATED")) {
            if (!readIf("ALWAYS")) {
                read("BY");
                read("DEFAULT");
            }
            read("AS");
            read("IDENTITY");
            SequenceOptions options = new SequenceOptions();
            if (readIf(OPEN_PAREN)) {
                parseSequenceOptions(options, null, true);
                read(CLOSE_PAREN);
            }
            column.setAutoIncrementOptions(options);
        }
        if (readIf(ON)) {
            read("UPDATE");
            Expression onUpdateExpression = readExpression();
            column.setOnUpdateExpression(session, onUpdateExpression);
        }
        if (NullConstraintType.NULL_IS_NOT_ALLOWED == parseNotNullConstraint()) {
            column.setNullable(false);
        }
        if (readIf("AUTO_INCREMENT") || readIf("BIGSERIAL") || readIf("SERIAL")) {
            parseAutoIncrement(column);
            parseNotNullConstraint();
        } else if (readIf("IDENTITY")) {
            parseAutoIncrement(column);
            column.setPrimaryKey(true);
            parseNotNullConstraint();
        }
        if (readIf("NULL_TO_DEFAULT")) {
            column.setConvertNullToDefault(true);
        }
        if (readIf("SEQUENCE")) {
            Sequence sequence = readSequence();
            column.setSequence(sequence);
        }
        if (readIf("SELECTIVITY")) {
            int value = readNonNegativeInt();
            column.setSelectivity(value);
        }
        String comment = readCommentIf();
        if (comment != null) {
            column.setComment(comment);
        }
        return column;
    }

    private void parseAutoIncrement(Column column) {
        SequenceOptions options = new SequenceOptions();
        if (readIf(OPEN_PAREN)) {
            options.setStartValue(ValueExpression.get(ValueLong.get(readLong())));
            if (readIf(COMMA)) {
                options.setIncrement(ValueExpression.get(ValueLong.get(readLong())));
            }
            read(CLOSE_PAREN);
        }
        column.setAutoIncrementOptions(options);
    }

    private String readCommentIf() {
        if (readIf("COMMENT")) {
            readIf(IS);
            return readString();
        }
        return null;
    }

    private Column parseColumnWithType(String columnName, boolean forTable) {
        String original = currentToken;
        boolean regular = false;
        int originalPrecision = -1, originalScale = -1;
        if (readIf("LONG")) {
            if (readIf("RAW")) {
                original = "LONG RAW";
            }
        } else if (readIf("DOUBLE")) {
            if (readIf("PRECISION")) {
                original = "DOUBLE PRECISION";
            }
        } else if (readIf("CHARACTER")) {
            if (readIf("VARYING")) {
                original = "CHARACTER VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "CHARACTER LARGE OBJECT";
            }
        } else if (readIf("BINARY")) {
            if (readIf("VARYING")) {
                original = "BINARY VARYING";
            } else if (readIf("LARGE")) {
                read("OBJECT");
                original = "BINARY LARGE OBJECT";
            }
        } else if (readIf("TIME")) {
            if (readIf(OPEN_PAREN)) {
                originalScale = readNonNegativeInt();
                if (originalScale > ValueTime.MAXIMUM_SCALE) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION, Integer.toString(originalScale));
                }
                read(CLOSE_PAREN);
            }
            if (readIf("WITHOUT")) {
                read("TIME");
                read("ZONE");
                original = "TIME WITHOUT TIME ZONE";
            }
        } else if (readIf("TIMESTAMP")) {
            if (readIf(OPEN_PAREN)) {
                originalScale = readNonNegativeInt();
                // Allow non-standard TIMESTAMP(..., ...) syntax
                if (readIf(COMMA)) {
                    originalScale = readNonNegativeInt();
                }
                if (originalScale > ValueTimestamp.MAXIMUM_SCALE) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION, Integer.toString(originalScale));
                }
                read(CLOSE_PAREN);
            }
            if (readIf(WITH)) {
                read("TIME");
                read("ZONE");
                original = "TIMESTAMP WITH TIME ZONE";
            } else if (readIf("WITHOUT")) {
                read("TIME");
                read("ZONE");
                original = "TIMESTAMP WITHOUT TIME ZONE";
            }
        } else if (readIf(INTERVAL)) {
            if (readIf("YEAR")) {
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                if (readIf("TO")) {
                    read("MONTH");
                    original = "INTERVAL YEAR TO MONTH";
                } else {
                    original = "INTERVAL YEAR";
                }
            } else if (readIf("MONTH")) {
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                original = "INTERVAL MONTH";
            } else if (readIf("DAY")) {
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                if (readIf("TO")) {
                    if (readIf("HOUR")) {
                        original = "INTERVAL DAY TO HOUR";
                    } else if (readIf("MINUTE")) {
                        original = "INTERVAL DAY TO MINUTE";
                    } else {
                        read("SECOND");
                        if (readIf(OPEN_PAREN)) {
                            originalScale = readNonNegativeInt();
                            read(CLOSE_PAREN);
                        }
                        original = "INTERVAL DAY TO SECOND";
                    }
                } else {
                    original = "INTERVAL DAY";
                }
            } else if (readIf("HOUR")) {
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                if (readIf("TO")) {
                    if (readIf("MINUTE")) {
                        original = "INTERVAL HOUR TO MINUTE";
                    } else {
                        read("SECOND");
                        if (readIf(OPEN_PAREN)) {
                            originalScale = readNonNegativeInt();
                            read(CLOSE_PAREN);
                        }
                        original = "INTERVAL HOUR TO SECOND";
                    }
                } else {
                    original = "INTERVAL HOUR";
                }
            } else if (readIf("MINUTE")) {
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    read(CLOSE_PAREN);
                }
                if (readIf("TO")) {
                    read("SECOND");
                    if (readIf(OPEN_PAREN)) {
                        originalScale = readNonNegativeInt();
                        read(CLOSE_PAREN);
                    }
                    original = "INTERVAL MINUTE TO SECOND";
                } else {
                    original = "INTERVAL MINUTE";
                }
            } else {
                read("SECOND");
                if (readIf(OPEN_PAREN)) {
                    originalPrecision = readNonNegativeInt();
                    if (readIf(COMMA)) {
                        originalScale = readNonNegativeInt();
                    }
                    read(CLOSE_PAREN);
                }
                original = "INTERVAL SECOND";
            }
        } else {
            regular = true;
        }
        long precision = -1;
        ExtTypeInfo extTypeInfo = null;
        int scale = -1;
        String comment = null;
        Column templateColumn = null;
        DataType dataType;
        if (!identifiersToUpper) {
            original = StringUtils.toUpperEnglish(original);
        }
        Domain domain = database.findDomain(original);
        if (domain != null) {
            templateColumn = domain.getColumn();
            TypeInfo type = templateColumn.getType();
            dataType = DataType.getDataType(type.getValueType());
            comment = templateColumn.getComment();
            original = forTable ? domain.getSQL(true) : templateColumn.getOriginalSQL();
            precision = type.getPrecision();
            scale = type.getScale();
            extTypeInfo = type.getExtTypeInfo();
        } else {
            Mode mode = database.getMode();
            dataType = DataType.getTypeByName(original, mode);
            if (dataType == null || mode.disallowedTypes.contains(original)) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                        currentToken);
            }
        }
        if (database.getIgnoreCase() && dataType.type == Value.STRING &&
                !equalsToken("VARCHAR_CASESENSITIVE", original)) {
            original = "VARCHAR_IGNORECASE";
            dataType = DataType.getTypeByName(original, database.getMode());
        }
        if (regular) {
            read();
        }
        precision = precision == -1 ? dataType.defaultPrecision : precision;
        scale = scale == -1 ? dataType.defaultScale : scale;
        if (dataType.supportsPrecision || dataType.supportsScale) {
            int t = dataType.type;
            if (t == Value.TIME || t == Value.TIMESTAMP || t == Value.TIMESTAMP_TZ) {
                if (originalScale >= 0) {
                    scale = originalScale;
                    switch (t) {
                    case Value.TIME:
                        if (original.equals("TIME WITHOUT TIME ZONE")) {
                            original = "TIME(" + originalScale + ") WITHOUT TIME ZONE";
                        } else {
                            original = original + '(' + originalScale + ')';
                        }
                        break;
                    case Value.TIMESTAMP:
                        if (original.equals("TIMESTAMP WITHOUT TIME ZONE")) {
                            original = "TIMESTAMP(" + originalScale + ") WITHOUT TIME ZONE";
                        } else {
                            original = original + '(' + originalScale + ')';
                        }
                        break;
                    case Value.TIMESTAMP_TZ:
                        original = "TIMESTAMP(" + originalScale + ") WITH TIME ZONE";
                        break;
                    }
                } else if (original.equals("DATETIME") || original.equals("DATETIME2")) {
                    if (readIf(OPEN_PAREN)) {
                        originalScale = readNonNegativeInt();
                        if (originalScale > ValueTime.MAXIMUM_SCALE) {
                            throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION,
                                    Integer.toString(originalScale));
                        }
                        read(CLOSE_PAREN);
                        scale = originalScale;
                        original = original + '(' + originalScale + ')';
                    }
                } else if (original.equals("SMALLDATETIME")) {
                    scale = 0;
                }
            } else if (DataType.isIntervalType(t)) {
                if (originalPrecision >= 0 || originalScale >= 0) {
                    IntervalQualifier qualifier = IntervalQualifier.valueOf(t - Value.INTERVAL_YEAR);
                    original = qualifier.getTypeName(originalPrecision, originalScale);
                    if (originalPrecision >= 0) {
                        if (originalPrecision <= 0 || originalPrecision > ValueInterval.MAXIMUM_PRECISION) {
                            throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION,
                                    Integer.toString(originalPrecision));
                        }
                        precision = originalPrecision;
                    }
                    if (originalScale >= 0) {
                        if (originalScale > ValueInterval.MAXIMUM_SCALE) {
                            throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION,
                                    Integer.toString(originalScale));
                        }
                        scale = originalScale;
                    }
                }
            } else if (readIf(OPEN_PAREN)) {
                if (!readIf("MAX")) {
                    long p = readPrecision();
                    original += "(" + p;
                    if (dataType.supportsScale) {
                        if (readIf(COMMA)) {
                            scale = readInt();
                            original += ", " + scale;
                        } else {
                            scale = 0;
                        }
                    }
                    precision = p;
                    original += ")";
                }
                read(CLOSE_PAREN);
            }
        } else if (dataType.type == Value.DOUBLE && original.equals("FLOAT")) {
            if (readIf(OPEN_PAREN)) {
                int p = readNonNegativeInt();
                read(CLOSE_PAREN);
                if (p > 53) {
                    throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION, Integer.toString(p));
                }
                if (p <= 24) {
                    dataType = DataType.getDataType(Value.FLOAT);
                }
                original = original + '(' + p + ')';
            }
        } else if (dataType.type == Value.ENUM) {
            if (extTypeInfo == null) {
                String[] enumerators = null;
                if (readIf(OPEN_PAREN)) {
                    java.util.List<String> enumeratorList = new ArrayList<>();
                    String enumerator0 = readString();
                    enumeratorList.add(enumerator0);
                    while (readIfMore(true)) {
                        String enumeratorN = readString();
                        enumeratorList.add(enumeratorN);
                    }
                    enumerators = enumeratorList.toArray(new String[0]);
                }
                try {
                    extTypeInfo = new ExtTypeInfoEnum(enumerators);
                } catch (DbException e) {
                    throw e.addSQL(original);
                }
                original += extTypeInfo.getCreateSQL();
            }
        } else if (dataType.type == Value.GEOMETRY) {
            if (extTypeInfo == null) {
                if (readIf(OPEN_PAREN)) {
                    int type = 0;
                    if (currentTokenType != IDENTIFIER || currentTokenQuoted) {
                        throw getSyntaxError();
                    }
                    if (!readIf("GEOMETRY")) {
                        try {
                            type = EWKTUtils.parseGeometryType(currentToken);
                            read();
                            if (type / 1_000 == 0 && currentTokenType == IDENTIFIER && !currentTokenQuoted) {
                                type +=  EWKTUtils.parseDimensionSystem(currentToken) * 1_000;
                                read();
                            }
                        } catch (IllegalArgumentException ex) {
                            throw getSyntaxError();
                        }
                    }
                    Integer srid = null;
                    if (readIf(COMMA)) {
                        srid = readInt();
                    }
                    read(CLOSE_PAREN);
                    extTypeInfo = new ExtTypeInfoGeometry(type, srid);
                    original += extTypeInfo.getCreateSQL();
                }
            }
        } else if (readIf(OPEN_PAREN)) {
            // Support for MySQL: INT(11), MEDIUMINT(8) and so on.
            // Just ignore the precision.
            readNonNegativeInt();
            read(CLOSE_PAREN);
        }
        if (readIf(FOR)) {
            read("BIT");
            read("DATA");
            if (dataType.type == Value.STRING) {
                dataType = DataType.getTypeByName("BINARY", database.getMode());
            }
        }
        // MySQL compatibility
        readIf("UNSIGNED");
        int type = dataType.type;
        if (scale > precision && dataType.supportsPrecision && dataType.supportsScale
                && !DataType.isIntervalType(type)) {
            throw DbException.get(ErrorCode.INVALID_VALUE_SCALE_PRECISION,
                    Integer.toString(scale), Long.toString(precision));
        }

        Column column = new Column(columnName, TypeInfo.getTypeInfo(type, precision, scale, extTypeInfo));
        if (templateColumn != null) {
            column.setNullable(templateColumn.isNullable());
            column.setDefaultExpression(session,
                    templateColumn.getDefaultExpression());
            int selectivity = templateColumn.getSelectivity();
            if (selectivity != Constants.SELECTIVITY_DEFAULT) {
                column.setSelectivity(selectivity);
            }
            Expression checkConstraint = templateColumn.getCheckConstraint(
                    session, columnName);
            column.addCheckConstraint(session, checkConstraint);
        }
        column.setComment(comment);
        column.setOriginalSQL(original);
        if (forTable) {
            column.setDomain(domain);
        }
        return column;
    }

    private long readPrecision() {
        long p = readNonNegativeLong();
        if (currentTokenType == IDENTIFIER && !currentTokenQuoted && currentToken.length() == 1) {
            long mul;
            char ch = currentToken.charAt(0);
            switch (identifiersToUpper ? ch : Character.toUpperCase(ch)) {
            case 'K':
                mul = 1L << 10;
                break;
            case 'M':
                mul = 1L << 20;
                break;
            case 'G':
                mul = 1L << 30;
                break;
            case 'T':
                mul = 1L << 40;
                break;
            case 'P':
                mul = 1L << 50;
                break;
            default:
                throw getSyntaxError();
            }
            if (p > Long.MAX_VALUE / mul) {
                throw DbException.getInvalidValueException("precision", p + currentToken);
            }
            p *= mul;
            read();
        }
        if (currentTokenType == IDENTIFIER && !currentTokenQuoted) {
            // Standard char length units
            if (!readIf("CHARACTERS") && !readIf("OCTETS") &&
                    // Oracle syntax
                    !readIf("CHAR")) {
                // Oracle syntax
                readIf("BYTE");
            }
        }
        return p;
    }

    private Prepared parseCreate() {
        boolean orReplace = false;
        if (readIf("OR")) {
            read("REPLACE");
            orReplace = true;
        }
        boolean force = readIf("FORCE");
        if (readIf("VIEW")) {
            return parseCreateView(force, orReplace);
        } else if (readIf("ALIAS")) {
            return parseCreateFunctionAlias(force);
        } else if (readIf("SEQUENCE")) {
            return parseCreateSequence();
        } else if (readIf("USER")) {
            return parseCreateUser();
        } else if (readIf("TRIGGER")) {
            return parseCreateTrigger(force);
        } else if (readIf("ROLE")) {
            return parseCreateRole();
        } else if (readIf("SCHEMA")) {
            return parseCreateSchema();
        } else if (readIf("CONSTANT")) {
            return parseCreateConstant();
        } else if (readIf("DOMAIN") || readIf("TYPE") || readIf("DATATYPE")) {
            return parseCreateDomain();
        } else if (readIf("AGGREGATE")) {
            return parseCreateAggregate(force);
        } else if (readIf("LINKED")) {
            return parseCreateLinkedTable(false, false, force);
        }
        // tables or linked tables
        boolean memory = false, cached = false;
        if (readIf("MEMORY")) {
            memory = true;
        } else if (readIf("CACHED")) {
            cached = true;
        }
        if (readIf("LOCAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, false, force);
            }
            read(TABLE);
            return parseCreateTable(true, false, cached);
        } else if (readIf("GLOBAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read(TABLE);
            return parseCreateTable(true, true, cached);
        } else if (readIf("TEMP") || readIf("TEMPORARY")) {
            if (readIf("LINKED")) {
                return parseCreateLinkedTable(true, true, force);
            }
            read(TABLE);
            return parseCreateTable(true, true, cached);
        } else if (readIf(TABLE)) {
            if (!cached && !memory) {
                cached = database.getDefaultTableType() == Table.TYPE_CACHED;
            }
            return parseCreateTable(false, false, cached);
        } else if (readIf("SYNONYM")) {
            return parseCreateSynonym(orReplace);
        } else {
            boolean hash = false, primaryKey = false;
            boolean unique = false, spatial = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if (readIf(PRIMARY)) {
                read("KEY");
                if (readIf("HASH")) {
                    hash = true;
                }
                primaryKey = true;
                if (!isToken(ON)) {
                    ifNotExists = readIfNotExists();
                    indexName = readIdentifierWithSchema(null);
                    oldSchema = getSchema();
                }
            } else {
                if (readIf(UNIQUE)) {
                    unique = true;
                }
                if (readIf("HASH")) {
                    hash = true;
                }
                if (readIf("SPATIAL")) {
                    spatial = true;
                }
                if (readIf("INDEX")) {
                    if (!isToken(ON)) {
                        ifNotExists = readIfNotExists();
                        indexName = readIdentifierWithSchema(null);
                        oldSchema = getSchema();
                    }
                } else {
                    throw getSyntaxError();
                }
            }
            read(ON);
            String tableName = readIdentifierWithSchema();
            checkSchema(oldSchema);
            CreateIndex command = new CreateIndex(session, getSchema());
            command.setIfNotExists(ifNotExists);
            command.setPrimaryKey(primaryKey);
            command.setTableName(tableName);
            command.setUnique(unique);
            command.setIndexName(indexName);
            command.setComment(readCommentIf());
            read(OPEN_PAREN);
            command.setIndexColumns(parseIndexColumnList());

            if (readIf("USING")) {
                if (hash) {
                    throw getSyntaxError();
                }
                if (spatial) {
                    throw getSyntaxError();
                }
                if (readIf("BTREE")) {
                    // default
                } else if (readIf("RTREE")) {
                    spatial = true;
                } else if (readIf("HASH")) {
                    hash = true;
                } else {
                    throw getSyntaxError();
                }

            }
            command.setHash(hash);
            command.setSpatial(spatial);
            return command;
        }
    }

    /**
     * @return true if we expect to see a TABLE clause
     */
    private boolean addRoleOrRight(GrantRevoke command) {
        if (readIf(SELECT)) {
            command.addRight(Right.SELECT);
            return true;
        } else if (readIf("DELETE")) {
            command.addRight(Right.DELETE);
            return true;
        } else if (readIf("INSERT")) {
            command.addRight(Right.INSERT);
            return true;
        } else if (readIf("UPDATE")) {
            command.addRight(Right.UPDATE);
            return true;
        } else if (readIf(ALL)) {
            command.addRight(Right.ALL);
            return true;
        } else if (readIf("ALTER")) {
            read("ANY");
            read("SCHEMA");
            command.addRight(Right.ALTER_ANY_SCHEMA);
            command.addTable(null);
            return false;
        } else if (readIf("CONNECT")) {
            // ignore this right
            return true;
        } else if (readIf("RESOURCE")) {
            // ignore this right
            return true;
        } else {
            command.addRoleName(readUniqueIdentifier());
            return false;
        }
    }

    private GrantRevoke parseGrantRevoke(int operationType) {
        GrantRevoke command = new GrantRevoke(session);
        command.setOperationType(operationType);
        boolean tableClauseExpected = addRoleOrRight(command);
        while (readIf(COMMA)) {
            addRoleOrRight(command);
            if (command.isRightMode() && command.isRoleMode()) {
                throw DbException
                        .get(ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED);
            }
        }
        if (tableClauseExpected) {
            if (readIf(ON)) {
                if (readIf("SCHEMA")) {
                    Schema schema = database.getSchema(readAliasIdentifier());
                    command.setSchema(schema);
                } else {
                    do {
                        Table table = readTableOrView();
                        command.addTable(table);
                    } while (readIf(COMMA));
                }
            }
        }
        if (operationType == CommandInterface.GRANT) {
            read("TO");
        } else {
            read(FROM);
        }
        command.setGranteeName(readUniqueIdentifier());
        return command;
    }

    private Select parseValues() {
        Select command = new Select(session, currentSelect);
        currentSelect = command;
        TableFilter filter = parseValuesTable(0);
        command.setWildcard();
        command.addTableFilter(filter, true);
        return command;
    }

    private TableFilter parseValuesTable(int orderInFrom) {
        Schema mainSchema = database.getMainSchema();
        TableFunction tf = (TableFunction) Function.getFunction(database, "TABLE");
        ArrayList<Column> columns = Utils.newSmallArrayList();
        ArrayList<ArrayList<Expression>> rows = Utils.newSmallArrayList();
        do {
            int i = 0;
            ArrayList<Expression> row = Utils.newSmallArrayList();
            boolean multiColumn;
            if (readIf(ROW)) {
                read(OPEN_PAREN);
                multiColumn = true;
            } else {
                multiColumn = readIf(OPEN_PAREN);
            }
            do {
                Expression expr = readExpression();
                expr = expr.optimize(session);
                TypeInfo type = expr.getType();
                Column column;
                String columnName = "C" + (i + 1);
                if (rows.isEmpty()) {
                    if (type.getValueType() == Value.UNKNOWN) {
                        type = TypeInfo.TYPE_STRING;
                    }
                    column = new Column(columnName, type);
                    columns.add(column);
                } else {
                    if (i >= columns.size()) {
                        throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                    }
                    type = Value.getHigherType(columns.get(i).getType(), type);
                    column = new Column(columnName, type);
                    columns.set(i, column);
                }
                row.add(expr);
                i++;
            } while (multiColumn && readIfMore(true));
            rows.add(row);
        } while (readIf(COMMA));
        int columnCount = columns.size();
        int rowCount = rows.size();
        for (ArrayList<Expression> row : rows) {
            if (row.size() != columnCount) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        for (int i = 0; i < columnCount; i++) {
            Column c = columns.get(i);
            if (c.getType().getValueType() == Value.UNKNOWN) {
                c = new Column(c.getName(), Value.STRING);
                columns.set(i, c);
            }
            Expression[] array = new Expression[rowCount];
            for (int j = 0; j < rowCount; j++) {
                array[j] = rows.get(j).get(i);
            }
            ExpressionList list = new ExpressionList(array, false);
            tf.setParameter(i, list);
        }
        tf.setColumns(columns);
        tf.doneWithParameters();
        Table table = new FunctionTable(mainSchema, session, tf, tf);
        return new TableFilter(session, table, null, rightsChecked, currentSelect, orderInFrom, null);
    }

    private Call parseCall() {
        Call command = new Call(session);
        currentPrepared = command;
        command.setExpression(readExpression());
        return command;
    }

    private CreateRole parseCreateRole() {
        CreateRole command = new CreateRole(session);
        command.setIfNotExists(readIfNotExists());
        command.setRoleName(readUniqueIdentifier());
        return command;
    }

    private CreateSchema parseCreateSchema() {
        CreateSchema command = new CreateSchema(session);
        command.setIfNotExists(readIfNotExists());
        command.setSchemaName(readUniqueIdentifier());
        if (readIf("AUTHORIZATION")) {
            command.setAuthorization(readUniqueIdentifier());
        } else {
            command.setAuthorization(session.getUser().getName());
        }
        if (readIf(WITH)) {
            command.setTableEngineParams(readTableEngineParams());
        }
        return command;
    }

    private ArrayList<String> readTableEngineParams() {
        ArrayList<String> tableEngineParams = Utils.newSmallArrayList();
        do {
            tableEngineParams.add(readUniqueIdentifier());
        } while (readIf(COMMA));
        return tableEngineParams;
    }

    private CreateSequence parseCreateSequence() {
        boolean ifNotExists = readIfNotExists();
        String sequenceName = readIdentifierWithSchema();
        CreateSequence command = new CreateSequence(session, getSchema());
        command.setIfNotExists(ifNotExists);
        command.setSequenceName(sequenceName);
        SequenceOptions options = new SequenceOptions();
        parseSequenceOptions(options, command, true);
        command.setOptions(options);
        return command;
    }

    private boolean readIfNotExists() {
        if (readIf(IF)) {
            read(NOT);
            read(EXISTS);
            return true;
        }
        return false;
    }

    private boolean readIfAffinity() {
        return readIf("AFFINITY") || readIf("SHARD");
    }

    private CreateConstant parseCreateConstant() {
        boolean ifNotExists = readIfNotExists();
        String constantName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (isKeyword(constantName)) {
            throw DbException.get(ErrorCode.CONSTANT_ALREADY_EXISTS_1,
                    constantName);
        }
        read("VALUE");
        Expression expr = readExpression();
        CreateConstant command = new CreateConstant(session, schema);
        command.setConstantName(constantName);
        command.setExpression(expr);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateAggregate parseCreateAggregate(boolean force) {
        boolean ifNotExists = readIfNotExists();
        CreateAggregate command = new CreateAggregate(session);
        command.setForce(force);
        String name = readIdentifierWithSchema();
        if (isKeyword(name) || Function.getFunction(database, name) != null ||
                getAggregateType(name) != null) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1,
                    name);
        }
        command.setName(name);
        command.setSchema(getSchema());
        command.setIfNotExists(ifNotExists);
        read(FOR);
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private CreateDomain parseCreateDomain() {
        boolean ifNotExists = readIfNotExists();
        CreateDomain command = new CreateDomain(session);
        command.setTypeName(readUniqueIdentifier());
        read("AS");
        Column col = parseColumnForTable("VALUE", true, false);
        if (readIf(CHECK)) {
            Expression expr = readExpression();
            col.addCheckConstraint(session, expr);
        }
        col.rename(null);
        command.setColumn(col);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateTrigger parseCreateTrigger(boolean force) {
        boolean ifNotExists = readIfNotExists();
        String triggerName = readIdentifierWithSchema(null);
        Schema schema = getSchema();
        boolean insteadOf, isBefore;
        if (readIf("INSTEAD")) {
            read("OF");
            isBefore = true;
            insteadOf = true;
        } else if (readIf("BEFORE")) {
            insteadOf = false;
            isBefore = true;
        } else {
            read("AFTER");
            insteadOf = false;
            isBefore = false;
        }
        int typeMask = 0;
        boolean onRollback = false;
        do {
            if (readIf("INSERT")) {
                typeMask |= Trigger.INSERT;
            } else if (readIf("UPDATE")) {
                typeMask |= Trigger.UPDATE;
            } else if (readIf("DELETE")) {
                typeMask |= Trigger.DELETE;
            } else if (readIf(SELECT)) {
                typeMask |= Trigger.SELECT;
            } else if (readIf("ROLLBACK")) {
                onRollback = true;
            } else {
                throw getSyntaxError();
            }
        } while (readIf(COMMA)
                || (database.getMode().getEnum() == ModeEnum.PostgreSQL
                        && readIf("OR")));
        read(ON);
        String tableName = readIdentifierWithSchema();
        checkSchema(schema);
        CreateTrigger command = new CreateTrigger(session, getSchema());
        command.setForce(force);
        command.setTriggerName(triggerName);
        command.setIfNotExists(ifNotExists);
        command.setInsteadOf(insteadOf);
        command.setBefore(isBefore);
        command.setOnRollback(onRollback);
        command.setTypeMask(typeMask);
        command.setTableName(tableName);
        if (readIf(FOR)) {
            read("EACH");
            read(ROW);
            command.setRowBased(true);
        } else {
            command.setRowBased(false);
        }
        if (readIf("QUEUE")) {
            command.setQueueSize(readNonNegativeInt());
        }
        command.setNoWait(readIf("NOWAIT"));
        if (readIf("AS")) {
            command.setTriggerSource(readString());
        } else {
            read("CALL");
            command.setTriggerClassName(readUniqueIdentifier());
        }
        return command;
    }

    private CreateUser parseCreateUser() {
        CreateUser command = new CreateUser(session);
        command.setIfNotExists(readIfNotExists());
        command.setUserName(readUniqueIdentifier());
        command.setComment(readCommentIf());
        if (readIf("PASSWORD")) {
            command.setPassword(readExpression());
        } else if (readIf("SALT")) {
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
        } else if (readIf("IDENTIFIED")) {
            read("BY");
            // uppercase if not quoted
            command.setPassword(ValueExpression.get(ValueString
                    .get(readColumnIdentifier())));
        } else {
            throw getSyntaxError();
        }
        if (readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private CreateFunctionAlias parseCreateFunctionAlias(boolean force) {
        boolean ifNotExists = readIfNotExists();
        String aliasName;
        if (currentTokenType != IDENTIFIER) {
            aliasName = currentToken;
            read();
            schemaName = session.getCurrentSchemaName();
        } else {
            aliasName = readIdentifierWithSchema();
        }
        final boolean newAliasSameNameAsBuiltin = Function.getFunction(database, aliasName) != null;
        if (database.isAllowBuiltinAliasOverride() && newAliasSameNameAsBuiltin) {
            // fine
        } else if (isKeyword(aliasName) ||
                newAliasSameNameAsBuiltin ||
                getAggregateType(aliasName) != null) {
            throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1,
                    aliasName);
        }
        CreateFunctionAlias command = new CreateFunctionAlias(session,
                getSchema());
        command.setForce(force);
        command.setAliasName(aliasName);
        command.setIfNotExists(ifNotExists);
        command.setDeterministic(readIf("DETERMINISTIC"));
        command.setBufferResultSetToLocalTemp(!readIf("NOBUFFER"));
        if (readIf("AS")) {
            command.setSource(readString());
        } else {
            read(FOR);
            command.setJavaClassMethod(readUniqueIdentifier());
        }
        return command;
    }

    private Prepared parseWith() {
        List<TableView> viewsCreated = new ArrayList<>();
        try {
            return parseWith1(viewsCreated);
        } catch (Throwable t) {
            CommandContainer.clearCTE(session, viewsCreated);
            throw t;
        }
    }

    private Prepared parseWith1(List<TableView> viewsCreated) {
        readIf("RECURSIVE");

        // This WITH statement is not a temporary view - it is part of a persistent view
        // as in CREATE VIEW abc AS WITH my_cte - this auto detects that condition.
        final boolean isTemporary = !session.isParsingCreateView();

        do {
            viewsCreated.add(parseSingleCommonTableExpression(isTemporary));
        } while (readIf(COMMA));

        Prepared p;
        // Reverse the order of constructed CTE views - as the destruction order
        // (since later created view may depend on previously created views -
        //  we preserve that dependency order in the destruction sequence )
        // used in setCteCleanups.
        Collections.reverse(viewsCreated);

        int parentheses = 0;
        while (readIf(OPEN_PAREN)) {
            parentheses++;
        }
        if (isToken(SELECT) || isToken(VALUES)) {
            p = parseWithQuery();
        } else if (isToken(TABLE)) {
            int index = lastParseIndex;
            read();
            if (!isToken(OPEN_PAREN)) {
                parseIndex = index;
                read();
                p = parseWithQuery();
            } else {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1, WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);
            }
        } else if (readIf("INSERT")) {
            p = parseInsert();
            p.setPrepareAlways(true);
        } else if (readIf("UPDATE")) {
            p = parseUpdate();
            p.setPrepareAlways(true);
        } else if (readIf("MERGE")) {
            p = parseMerge();
            p.setPrepareAlways(true);
        } else if (readIf("DELETE")) {
            p = parseDelete();
            p.setPrepareAlways(true);
        } else if (readIf("CREATE")) {
            if (!isToken(TABLE)) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                        WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);

            }
            p = parseCreate();
            p.setPrepareAlways(true);
        } else {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                    WITH_STATEMENT_SUPPORTS_LIMITED_SUB_STATEMENTS);
        }
        for (; parentheses > 0; parentheses--) {
            read(CLOSE_PAREN);
        }

        // Clean up temporary views starting with last to first (in case of
        // dependencies) - but only if they are not persistent.
        if (isTemporary) {
            p.setCteCleanups(viewsCreated);
        }
        return p;
    }

    private Prepared parseWithQuery() {
        Query query = parseSelectUnion();
        query.setPrepareAlways(true);
        query.setNeverLazy(true);
        return query;
    }

    private TableView parseSingleCommonTableExpression(boolean isTemporary) {
        String cteViewName = readIdentifierWithSchema();
        Schema schema = getSchema();
        ArrayList<Column> columns = Utils.newSmallArrayList();
        String[] cols = null;

        // column names are now optional - they can be inferred from the named
        // query, if not supplied by user
        if (readIf(OPEN_PAREN)) {
            cols = parseColumnList();
            for (String c : cols) {
                // we don't really know the type of the column, so STRING will
                // have to do, UNKNOWN does not work here
                columns.add(new Column(c, Value.STRING));
            }
        }

        Table oldViewFound;
        if (!isTemporary) {
            oldViewFound = getSchema().findTableOrView(session, cteViewName);
        } else {
            oldViewFound = session.findLocalTempTable(cteViewName);
        }
        // this persistent check conflicts with check 10 lines down
        if (oldViewFound != null) {
            if (!(oldViewFound instanceof TableView)) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                        cteViewName);
            }
            TableView tv = (TableView) oldViewFound;
            if (!tv.isTableExpression()) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                        cteViewName);
            }
            if (!isTemporary) {
                oldViewFound.lock(session, true, true);
                database.removeSchemaObject(session, oldViewFound);

            } else {
                session.removeLocalTempTable(oldViewFound);
            }
        }
        /*
         * This table is created as a workaround because recursive table
         * expressions need to reference something that look like themselves to
         * work (its removed after creation in this method). Only create table
         * data and table if we don't have a working CTE already.
         */
        Table recursiveTable = TableView.createShadowTableForRecursiveTableExpression(
                isTemporary, session, cteViewName, schema, columns, database);
        List<Column> columnTemplateList;
        String[] querySQLOutput = {null};
        try {
            read("AS");
            read(OPEN_PAREN);
            Query withQuery = parseSelect();
            if (!isTemporary) {
                withQuery.session = session;
            }
            read(CLOSE_PAREN);
            columnTemplateList = TableView.createQueryColumnTemplateList(cols, withQuery, querySQLOutput);

        } finally {
            TableView.destroyShadowTableForRecursiveExpression(isTemporary, session, recursiveTable);
        }

        return createCTEView(cteViewName,
                querySQLOutput[0], columnTemplateList,
                true/* allowRecursiveQueryDetection */,
                true/* add to session */,
                isTemporary);
    }

    private TableView createCTEView(String cteViewName, String querySQL,
                                    List<Column> columnTemplateList, boolean allowRecursiveQueryDetection,
                                    boolean addViewToSession, boolean isTemporary) {
        Database db = session.getDatabase();
        Schema schema = getSchemaWithDefault();
        int id = db.allocateObjectId();
        Column[] columnTemplateArray = columnTemplateList.toArray(new Column[0]);

        // No easy way to determine if this is a recursive query up front, so we just compile
        // it twice - once without the flag set, and if we didn't see a recursive term,
        // then we just compile it again.
        TableView view;
        synchronized (session) {
            view = new TableView(schema, id, cteViewName, querySQL,
                    parameters, columnTemplateArray, session,
                    allowRecursiveQueryDetection, false /* literalsChecked */, true /* isTableExpression */,
                    isTemporary);
            if (!view.isRecursiveQueryDetected() && allowRecursiveQueryDetection) {
                if (!isTemporary) {
                    db.addSchemaObject(session, view);
                    view.lock(session, true, true);
                    db.removeSchemaObject(session, view);
                } else {
                    session.removeLocalTempTable(view);
                }
                view = new TableView(schema, id, cteViewName, querySQL, parameters,
                        columnTemplateArray, session,
                        false/* assume recursive */, false /* literalsChecked */, true /* isTableExpression */,
                        isTemporary);
            }
            // both removeSchemaObject and removeLocalTempTable hold meta locks
            db.unlockMeta(session);
        }
        view.setTableExpression(true);
        view.setTemporary(isTemporary);
        view.setHidden(true);
        view.setOnCommitDrop(false);
        if (addViewToSession) {
            if (!isTemporary) {
                db.addSchemaObject(session, view);
                view.unlock(session);
                db.unlockMeta(session);
            } else {
                session.addLocalTempTable(view);
            }
        }
        return view;
    }

    private CreateView parseCreateView(boolean force, boolean orReplace) {
        boolean ifNotExists = readIfNotExists();
        boolean isTableExpression = readIf("TABLE_EXPRESSION");
        String viewName = readIdentifierWithSchema();
        CreateView command = new CreateView(session, getSchema());
        this.createView = command;
        command.setViewName(viewName);
        command.setIfNotExists(ifNotExists);
        command.setComment(readCommentIf());
        command.setOrReplace(orReplace);
        command.setForce(force);
        command.setTableExpression(isTableExpression);
        if (readIf(OPEN_PAREN)) {
            String[] cols = parseColumnList();
            command.setColumnNames(cols);
        }
        String select = StringUtils.cache(sqlCommand
                .substring(parseIndex));
        read("AS");
        try {
            Query query;
            session.setParsingCreateView(true, viewName);
            try {
                query = parseSelect();
                query.prepare();
            } finally {
                session.setParsingCreateView(false, viewName);
            }
            command.setSelect(query);
        } catch (DbException e) {
            if (force) {
                command.setSelectSQL(select);
                while (currentTokenType != END) {
                    read();
                }
            } else {
                throw e;
            }
        }
        return command;
    }

    private TransactionCommand parseCheckpoint() {
        TransactionCommand command;
        if (readIf("SYNC")) {
            command = new TransactionCommand(session,
                    CommandInterface.CHECKPOINT_SYNC);
        } else {
            command = new TransactionCommand(session,
                    CommandInterface.CHECKPOINT);
        }
        return command;
    }

    private Prepared parseAlter() {
        if (readIf(TABLE)) {
            return parseAlterTable();
        } else if (readIf("USER")) {
            return parseAlterUser();
        } else if (readIf("INDEX")) {
            return parseAlterIndex();
        } else if (readIf("SCHEMA")) {
            return parseAlterSchema();
        } else if (readIf("SEQUENCE")) {
            return parseAlterSequence();
        } else if (readIf("VIEW")) {
            return parseAlterView();
        }
        throw getSyntaxError();
    }

    private void checkSchema(Schema old) {
        if (old != null && getSchema() != old) {
            throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH);
        }
    }

    private AlterIndexRename parseAlterIndex() {
        boolean ifExists = readIfExists(false);
        String indexName = readIdentifierWithSchema();
        Schema old = getSchema();
        AlterIndexRename command = new AlterIndexRename(session);
        command.setOldSchema(old);
        command.setOldName(indexName);
        command.setIfExists(ifExists);
        read("RENAME");
        read("TO");
        String newName = readIdentifierWithSchema(old.getName());
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private DefineCommand parseAlterView() {
        boolean ifExists = readIfExists(false);
        String viewName = readIdentifierWithSchema();
        Schema schema = getSchema();
        Table tableView = schema.findTableOrView(session, viewName);
        if (!(tableView instanceof TableView) && !ifExists) {
            throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
        }
        if (readIf("RENAME")) {
            read("TO");
            String newName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            AlterTableRename command = new AlterTableRename(session, getSchema());
            command.setOldTableName(viewName);
            command.setNewTableName(newName);
            command.setIfTableExists(ifExists);
            return command;
        } else {
            read("RECOMPILE");
            TableView view = (TableView) tableView;
            AlterView command = new AlterView(session);
            command.setIfExists(ifExists);
            command.setView(view);
            return command;
        }
    }

    private Prepared parseAlterSchema() {
        boolean ifExists = readIfExists(false);
        String schemaName = readIdentifierWithSchema();
        Schema old = getSchema();
        read("RENAME");
        read("TO");
        String newName = readIdentifierWithSchema(old.getName());
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            if (ifExists) {
                return new NoOperation(session);
            }
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        AlterSchemaRename command = new AlterSchemaRename(session);
        command.setOldSchema(schema);
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private AlterSequence parseAlterSequence() {
        boolean ifExists = readIfExists(false);
        String sequenceName = readIdentifierWithSchema();
        AlterSequence command = new AlterSequence(session, getSchema());
        command.setSequenceName(sequenceName);
        command.setIfExists(ifExists);
        SequenceOptions options = new SequenceOptions();
        parseSequenceOptions(options, null, false);
        command.setOptions(options);
        return command;
    }

    private void parseSequenceOptions(SequenceOptions options, CreateSequence command, boolean forCreate) {
        for (;;) {
            if (readIf(forCreate ? "START" : "RESTART")) {
                readIf(WITH);
                options.setStartValue(readExpression());
            } else if (readIf("INCREMENT")) {
                readIf("BY");
                options.setIncrement(readExpression());
            } else if (readIf("MINVALUE")) {
                options.setMinValue(readExpression());
            } else if (readIf("NOMINVALUE")) {
                options.setMinValue(ValueExpression.getNull());
            } else if (readIf("MAXVALUE")) {
                options.setMaxValue(readExpression());
            } else if (readIf("NOMAXVALUE")) {
                options.setMaxValue(ValueExpression.getNull());
            } else if (readIf("CYCLE")) {
                options.setCycle(true);
            } else if (readIf("NOCYCLE")) {
                options.setCycle(false);
            } else if (readIf("NO")) {
                if (readIf("MINVALUE")) {
                    options.setMinValue(ValueExpression.getNull());
                } else if (readIf("MAXVALUE")) {
                    options.setMaxValue(ValueExpression.getNull());
                } else if (readIf("CYCLE")) {
                    options.setCycle(false);
                } else if (readIf("CACHE")) {
                    options.setCacheSize(ValueExpression.get(ValueLong.get(1)));
                } else {
                    break;
                }
            } else if (readIf("CACHE")) {
                options.setCacheSize(readExpression());
            } else if (readIf("NOCACHE")) {
                options.setCacheSize(ValueExpression.get(ValueLong.get(1)));
            } else if (command != null) {
                if (readIf("BELONGS_TO_TABLE")) {
                    command.setBelongsToTable(true);
                } else if (readIf(ORDER)) {
                    // Oracle compatibility
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    private AlterUser parseAlterUser() {
        String userName = readUniqueIdentifier();
        if (readIf("SET")) {
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
            command.setUser(database.getUser(userName));
            if (readIf("PASSWORD")) {
                command.setPassword(readExpression());
            } else if (readIf("SALT")) {
                command.setSalt(readExpression());
                read("HASH");
                command.setHash(readExpression());
            } else {
                throw getSyntaxError();
            }
            return command;
        } else if (readIf("RENAME")) {
            read("TO");
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_RENAME);
            command.setUser(database.getUser(userName));
            String newName = readUniqueIdentifier();
            command.setNewName(newName);
            return command;
        } else if (readIf("ADMIN")) {
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_ADMIN);
            User user = database.getUser(userName);
            command.setUser(user);
            if (readIf(TRUE)) {
                command.setAdmin(true);
            } else if (readIf(FALSE)) {
                command.setAdmin(false);
            } else {
                throw getSyntaxError();
            }
            return command;
        }
        throw getSyntaxError();
    }

    private void readIfEqualOrTo() {
        if (!readIf(EQUAL)) {
            readIf("TO");
        }
    }

    private Prepared parseSet() {
        if (readIf(AT)) {
            Set command = new Set(session, SetTypes.VARIABLE);
            command.setString(readAliasIdentifier());
            readIfEqualOrTo();
            command.setExpression(readExpression());
            return command;
        } else if (readIf("AUTOCOMMIT")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            int setting = value ? CommandInterface.SET_AUTOCOMMIT_TRUE
                    : CommandInterface.SET_AUTOCOMMIT_FALSE;
            return new TransactionCommand(session, setting);
        } else if (readIf("MVCC")) {
            readIfEqualOrTo();
            readBooleanSetting();
            return new NoOperation(session);
        } else if (readIf("EXCLUSIVE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.EXCLUSIVE);
            command.setExpression(readExpression());
            return command;
        } else if (readIf("IGNORECASE")) {
            readIfEqualOrTo();
            boolean value = readBooleanSetting();
            Set command = new Set(session, SetTypes.IGNORECASE);
            command.setInt(value ? 1 : 0);
            return command;
        } else if (readIf("PASSWORD")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
            command.setUser(session.getUser());
            command.setPassword(readExpression());
            return command;
        } else if (readIf("SALT")) {
            readIfEqualOrTo();
            AlterUser command = new AlterUser(session);
            command.setType(CommandInterface.ALTER_USER_SET_PASSWORD);
            command.setUser(session.getUser());
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
            return command;
        } else if (readIf("MODE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.MODE);
            command.setString(readAliasIdentifier());
            return command;
        } else if (readIf("COMPRESS_LOB")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.COMPRESS_LOB);
            if (currentTokenType == VALUE) {
                command.setString(readString());
            } else {
                command.setString(readUniqueIdentifier());
            }
            return command;
        } else if (readIf("DATABASE")) {
            readIfEqualOrTo();
            read("COLLATION");
            return parseSetCollation();
        } else if (readIf("COLLATION")) {
            readIfEqualOrTo();
            return parseSetCollation();
        } else if (readIf("BINARY_COLLATION")) {
            readIfEqualOrTo();
            return parseSetBinaryCollation(SetTypes.BINARY_COLLATION);
        } else if (readIf("UUID_COLLATION")) {
            readIfEqualOrTo();
            return parseSetBinaryCollation(SetTypes.UUID_COLLATION);
        } else if (readIf("CLUSTER")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.CLUSTER);
            command.setString(readString());
            return command;
        } else if (readIf("DATABASE_EVENT_LISTENER")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DATABASE_EVENT_LISTENER);
            command.setString(readString());
            return command;
        } else if (readIf("ALLOW_LITERALS")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.ALLOW_LITERALS);
            if (readIf("NONE")) {
                command.setInt(Constants.ALLOW_LITERALS_NONE);
            } else if (readIf(ALL)) {
                command.setInt(Constants.ALLOW_LITERALS_ALL);
            } else if (readIf("NUMBERS")) {
                command.setInt(Constants.ALLOW_LITERALS_NUMBERS);
            } else {
                command.setInt(readNonNegativeInt());
            }
            return command;
        } else if (readIf("DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.DEFAULT_TABLE_TYPE);
            if (readIf("MEMORY")) {
                command.setInt(Table.TYPE_MEMORY);
            } else if (readIf("CACHED")) {
                command.setInt(Table.TYPE_CACHED);
            } else {
                command.setInt(readNonNegativeInt());
            }
            return command;
        } else if (readIf("CREATE")) {
            readIfEqualOrTo();
            // Derby compatibility (CREATE=TRUE in the database URL)
            read();
            return new NoOperation(session);
        } else if (readIf("HSQLDB.DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("PAGE_STORE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("CACHE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("FILE_LOCK")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("DB_CLOSE_ON_EXIT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("AUTO_SERVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("AUTO_SERVER_PORT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("AUTO_RECONNECT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("ASSERT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("ACCESS_MODE_DATA")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("OPEN_NEW")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("JMX")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("PAGE_SIZE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("RECOVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("NAMES")) {
            // Quercus PHP MySQL driver compatibility
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("SCOPE_GENERATED_KEYS")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("SCHEMA")) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA);
            command.setString(readAliasIdentifier());
            return command;
        } else if (readIf("DATESTYLE")) {
            // PostgreSQL compatibility
            readIfEqualOrTo();
            if (!readIf("ISO")) {
                String s = readString();
                if (!equalsToken(s, "ISO")) {
                    throw getSyntaxError();
                }
            }
            return new NoOperation(session);
        } else if (readIf("SEARCH_PATH") ||
                readIf(SetTypes.getTypeName(SetTypes.SCHEMA_SEARCH_PATH))) {
            readIfEqualOrTo();
            Set command = new Set(session, SetTypes.SCHEMA_SEARCH_PATH);
            ArrayList<String> list = Utils.newSmallArrayList();
            do {
                list.add(readAliasIdentifier());
            } while (readIf(COMMA));
            command.setStringArray(list.toArray(new String[0]));
            return command;
        } else if (readIf("JAVA_OBJECT_SERIALIZER")) {
            readIfEqualOrTo();
            return parseSetJavaObjectSerializer();
        } else {
            if (isToken("LOGSIZE")) {
                // HSQLDB compatibility
                currentToken = SetTypes.getTypeName(SetTypes.MAX_LOG_SIZE);
            }
            if (isToken("FOREIGN_KEY_CHECKS")) {
                // MySQL compatibility
                currentToken = SetTypes
                        .getTypeName(SetTypes.REFERENTIAL_INTEGRITY);
            }
            String typeName = currentToken;
            if (!identifiersToUpper) {
                typeName = StringUtils.toUpperEnglish(typeName);
            }
            int type = SetTypes.getType(typeName);
            if (type < 0) {
                throw getSyntaxError();
            }
            read();
            readIfEqualOrTo();
            Set command = new Set(session, type);
            command.setExpression(readExpression());
            return command;
        }
    }

    private Prepared parseUse() {
        readIfEqualOrTo();
        Set command = new Set(session, SetTypes.SCHEMA);
        command.setString(readAliasIdentifier());
        return command;
    }

    private Set parseSetCollation() {
        Set command = new Set(session, SetTypes.COLLATION);
        String name = readAliasIdentifier();
        command.setString(name);
        if (equalsToken(name, CompareMode.OFF)) {
            return command;
        }
        Collator coll = CompareMode.getCollator(name);
        if (coll == null) {
            throw DbException.getInvalidValueException("collation", name);
        }
        if (readIf("STRENGTH")) {
            if (readIf(PRIMARY)) {
                command.setInt(Collator.PRIMARY);
            } else if (readIf("SECONDARY")) {
                command.setInt(Collator.SECONDARY);
            } else if (readIf("TERTIARY")) {
                command.setInt(Collator.TERTIARY);
            } else if (readIf("IDENTICAL")) {
                command.setInt(Collator.IDENTICAL);
            }
        } else {
            command.setInt(coll.getStrength());
        }
        return command;
    }

    private Set parseSetBinaryCollation(int type) {
        String name = readAliasIdentifier();
        if (equalsToken(name, CompareMode.UNSIGNED) || equalsToken(name, CompareMode.SIGNED)) {
            Set command = new Set(session, type);
            command.setString(name);
            return command;
        }
        throw DbException.getInvalidValueException(SetTypes.getTypeName(type), name);
    }

    private Set parseSetJavaObjectSerializer() {
        Set command = new Set(session, SetTypes.JAVA_OBJECT_SERIALIZER);
        String name = readString();
        command.setString(name);
        return command;
    }

    private RunScriptCommand parseRunScript() {
        RunScriptCommand command = new RunScriptCommand(session);
        read(FROM);
        command.setFileNameExpr(readExpression());
        if (readIf("COMPRESSION")) {
            command.setCompressionAlgorithm(readUniqueIdentifier());
        }
        if (readIf("CIPHER")) {
            command.setCipher(readUniqueIdentifier());
            if (readIf("PASSWORD")) {
                command.setPassword(readExpression());
            }
        }
        if (readIf("CHARSET")) {
            command.setCharset(Charset.forName(readString()));
        }
        return command;
    }

    private ScriptCommand parseScript() {
        ScriptCommand command = new ScriptCommand(session);
        boolean data = true, passwords = true, settings = true;
        boolean dropTables = false, simple = false, withColumns = false;
        if (readIf("NODATA")) {
            data = false;
        } else {
            if (readIf("SIMPLE")) {
                simple = true;
            }
            if (readIf("COLUMNS")) {
                withColumns = true;
            }
        }
        if (readIf("NOPASSWORDS")) {
            passwords = false;
        }
        if (readIf("NOSETTINGS")) {
            settings = false;
        }
        if (readIf("DROP")) {
            dropTables = true;
        }
        if (readIf("BLOCKSIZE")) {
            long blockSize = readLong();
            command.setLobBlockSize(blockSize);
        }
        command.setData(data);
        command.setPasswords(passwords);
        command.setSettings(settings);
        command.setDrop(dropTables);
        command.setSimple(simple);
        command.setWithColumns(withColumns);
        if (readIf("TO")) {
            command.setFileNameExpr(readExpression());
            if (readIf("COMPRESSION")) {
                command.setCompressionAlgorithm(readUniqueIdentifier());
            }
            if (readIf("CIPHER")) {
                command.setCipher(readUniqueIdentifier());
                if (readIf("PASSWORD")) {
                    command.setPassword(readExpression());
                }
            }
            if (readIf("CHARSET")) {
                command.setCharset(Charset.forName(readString()));
            }
        }
        if (readIf("SCHEMA")) {
            HashSet<String> schemaNames = new HashSet<>();
            do {
                schemaNames.add(readUniqueIdentifier());
            } while (readIf(COMMA));
            command.setSchemaNames(schemaNames);
        } else if (readIf(TABLE)) {
            ArrayList<Table> tables = Utils.newSmallArrayList();
            do {
                tables.add(readTableOrView());
            } while (readIf(COMMA));
            command.setTables(tables);
        }
        return command;
    }

    /**
     * Is this the Oracle DUAL table or the IBM/DB2 SYSIBM table?
     *
     * @param tableName table name.
     * @return {@code true} if the table is DUAL special table. Otherwise returns {@code false}.
     * @see <a href="https://en.wikipedia.org/wiki/DUAL_table">Wikipedia: DUAL table</a>
     */
    boolean isDualTable(String tableName) {
        return ((schemaName == null || equalsToken(schemaName, "SYS")) && equalsToken("DUAL", tableName))
                || (database.getMode().sysDummy1 && (schemaName == null || equalsToken(schemaName, "SYSIBM"))
                        && equalsToken("SYSDUMMY1", tableName));
    }

    private Table readTableOrView() {
        return readTableOrView(readIdentifierWithSchema(null));
    }

    private Table readTableOrView(String tableName) {
        if (schemaName != null) {
            Table table = getSchema().resolveTableOrView(session, tableName);
            if (table != null) {
                return table;
            }
        } else {
            Table table = database.getSchema(session.getCurrentSchemaName())
                    .resolveTableOrView(session, tableName);
            if (table != null) {
                return table;
            }
            String[] schemaNames = session.getSchemaSearchPath();
            if (schemaNames != null) {
                for (String name : schemaNames) {
                    Schema s = database.getSchema(name);
                    table = s.resolveTableOrView(session, tableName);
                    if (table != null) {
                        return table;
                    }
                }
            }
        }
        if (isDualTable(tableName)) {
            return getDualTable(false);
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

    private FunctionAlias findFunctionAlias(String schema, String aliasName) {
        FunctionAlias functionAlias = database.getSchema(schema).findFunction(
                aliasName);
        if (functionAlias != null) {
            return functionAlias;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        if (schemaNames != null) {
            for (String n : schemaNames) {
                functionAlias = database.getSchema(n).findFunction(aliasName);
                if (functionAlias != null) {
                    return functionAlias;
                }
            }
        }
        return null;
    }

    private Sequence findSequence(String schema, String sequenceName) {
        Sequence sequence = database.getSchema(schema).findSequence(
                sequenceName);
        if (sequence != null) {
            return sequence;
        }
        String[] schemaNames = session.getSchemaSearchPath();
        if (schemaNames != null) {
            for (String n : schemaNames) {
                sequence = database.getSchema(n).findSequence(sequenceName);
                if (sequence != null) {
                    return sequence;
                }
            }
        }
        return null;
    }

    private Sequence readSequence() {
        // same algorithm as readTableOrView
        String sequenceName = readIdentifierWithSchema(null);
        if (schemaName != null) {
            return getSchema().getSequence(sequenceName);
        }
        Sequence sequence = findSequence(session.getCurrentSchemaName(),
                sequenceName);
        if (sequence != null) {
            return sequence;
        }
        throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
    }

    private Prepared parseAlterTable() {
        boolean ifTableExists = readIfExists(false);
        String tableName = readIdentifierWithSchema();
        Schema schema = getSchema();
        if (readIf("ADD")) {
            Prepared command = parseAlterTableAddConstraintIf(tableName,
                    schema, ifTableExists);
            if (command != null) {
                return command;
            }
            return parseAlterTableAddColumn(tableName, schema, ifTableExists);
        } else if (readIf("SET")) {
            read("REFERENTIAL_INTEGRITY");
            int type = CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY;
            boolean value = readBooleanSetting();
            AlterTableSet command = new AlterTableSet(session,
                    schema, type, value);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            if (readIf(CHECK)) {
                command.setCheckExisting(true);
            } else if (readIf("NOCHECK")) {
                command.setCheckExisting(false);
            }
            return command;
        } else if (readIf("RENAME")) {
            if (readIf("COLUMN")) {
                // PostgreSQL syntax
                String columnName = readColumnIdentifier();
                read("TO");
                AlterTableRenameColumn command = new AlterTableRenameColumn(
                        session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumnName(columnName);
                String newName = readColumnIdentifier();
                command.setNewColumnName(newName);
                return command;
            } else if (readIf(CONSTRAINT)) {
                String constraintName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                read("TO");
                AlterTableRenameConstraint command = new AlterTableRenameConstraint(
                        session, schema);
                command.setConstraintName(constraintName);
                String newName = readColumnIdentifier();
                command.setNewConstraintName(newName);
                return commandIfTableExists(schema, tableName, ifTableExists, command);
            } else {
                read("TO");
                String newName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                AlterTableRename command = new AlterTableRename(session,
                        getSchema());
                command.setOldTableName(tableName);
                command.setNewTableName(newName);
                command.setIfTableExists(ifTableExists);
                command.setHidden(readIf("HIDDEN"));
                return command;
            }
        } else if (readIf("DROP")) {
            if (readIf(CONSTRAINT)) {
                boolean ifExists = readIfExists(false);
                String constraintName = readIdentifierWithSchema(schema.getName());
                ifExists = readIfExists(ifExists);
                checkSchema(schema);
                AlterTableDropConstraint command = new AlterTableDropConstraint(
                        session, getSchema(), ifExists);
                command.setConstraintName(constraintName);
                return commandIfTableExists(schema, tableName, ifTableExists, command);
            } else if (readIf(FOREIGN)) {
                // MySQL compatibility
                read("KEY");
                String constraintName = readIdentifierWithSchema(schema.getName());
                checkSchema(schema);
                AlterTableDropConstraint command = new AlterTableDropConstraint(
                        session, getSchema(), false);
                command.setConstraintName(constraintName);
                return commandIfTableExists(schema, tableName, ifTableExists, command);
            } else if (readIf("INDEX")) {
                // MySQL compatibility
                String indexOrConstraintName = readIdentifierWithSchema(schema.getName());
                final SchemaCommand command;
                if (schema.findIndex(session, indexOrConstraintName) != null) {
                    DropIndex dropIndexCommand = new DropIndex(session, getSchema());
                    dropIndexCommand.setIndexName(indexOrConstraintName);
                    command = dropIndexCommand;
                } else {
                    AlterTableDropConstraint dropCommand = new AlterTableDropConstraint(
                            session, getSchema(), false/*ifExists*/);
                    dropCommand.setConstraintName(indexOrConstraintName);
                    command = dropCommand;
                }
                return commandIfTableExists(schema, tableName, ifTableExists, command);
            } else if (readIf(PRIMARY)) {
                read("KEY");
                Table table = tableIfTableExists(schema, tableName, ifTableExists);
                if (table == null) {
                    return new NoOperation(session);
                }
                Index idx = table.getPrimaryKey();
                DropIndex command = new DropIndex(session, schema);
                command.setIndexName(idx.getName());
                return command;
            } else {
                readIf("COLUMN");
                boolean ifExists = readIfExists(false);
                ArrayList<Column> columnsToRemove = new ArrayList<>();
                Table table = tableIfTableExists(schema, tableName, ifTableExists);
                // For Oracle compatibility - open bracket required
                boolean openingBracketDetected = readIf(OPEN_PAREN);
                do {
                    String columnName = readColumnIdentifier();
                    if (table != null) {
                        if (!ifExists || table.doesColumnExist(columnName)) {
                            Column column = table.getColumn(columnName);
                            columnsToRemove.add(column);
                        }
                    }
                } while (readIf(COMMA));
                if (openingBracketDetected) {
                    // For Oracle compatibility - close bracket
                    read(CLOSE_PAREN);
                }
                if (table == null || columnsToRemove.isEmpty()) {
                    return new NoOperation(session);
                }
                AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
                command.setType(CommandInterface.ALTER_TABLE_DROP_COLUMN);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setColumnsToRemove(columnsToRemove);
                return command;
            }
        } else if (readIf("CHANGE")) {
            // MySQL compatibility
            readIf("COLUMN");
            String columnName = readColumnIdentifier();
            String newColumnName = readColumnIdentifier();
            Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists);
            boolean nullable = column == null ? true : column.isNullable();
            // new column type ignored. RENAME and MODIFY are
            // a single command in MySQL but two different commands in H2.
            parseColumnForTable(newColumnName, nullable, true);
            AlterTableRenameColumn command = new AlterTableRenameColumn(session, schema);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            command.setOldColumnName(columnName);
            command.setNewColumnName(newColumnName);
            return command;
        } else if (readIf("MODIFY")) {
            // MySQL compatibility (optional)
            readIf("COLUMN");
            // Oracle specifies (but will not require) an opening parenthesis
            boolean hasOpeningBracket = readIf(OPEN_PAREN);
            String columnName = readColumnIdentifier();
            AlterTableAlterColumn command;
            NullConstraintType nullConstraint = parseNotNullConstraint();
            switch (nullConstraint) {
            case NULL_IS_ALLOWED:
            case NULL_IS_NOT_ALLOWED:
                command = new AlterTableAlterColumn(session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists);
                command.setOldColumn(column);
                if (nullConstraint == NullConstraintType.NULL_IS_ALLOWED) {
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
                } else {
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL);
                }
                break;
            case NO_NULL_CONSTRAINT_FOUND:
                command = parseAlterTableAlterColumnType(schema, tableName, columnName, ifTableExists);
                break;
            default:
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                        "Internal Error - unhandled case: " + nullConstraint.name());
            }
            if(hasOpeningBracket) {
                read(CLOSE_PAREN);
            }
            return command;
        } else if (readIf("ALTER")) {
            readIf("COLUMN");
            String columnName = readColumnIdentifier();
            Column column = columnIfTableExists(schema, tableName, columnName, ifTableExists);
            if (readIf("RENAME")) {
                read("TO");
                AlterTableRenameColumn command = new AlterTableRenameColumn(
                        session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumnName(columnName);
                String newName = readColumnIdentifier();
                command.setNewColumnName(newName);
                return command;
            } else if (readIf("DROP")) {
                if (readIf("DEFAULT")) {
                    AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
                    command.setTableName(tableName);
                    command.setIfTableExists(ifTableExists);
                    command.setOldColumn(column);
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT);
                    command.setDefaultExpression(null);
                    return command;
                }
                if (readIf(ON)) {
                    read("UPDATE");
                    AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
                    command.setTableName(tableName);
                    command.setIfTableExists(ifTableExists);
                    command.setOldColumn(column);
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_ON_UPDATE);
                    command.setDefaultExpression(null);
                    return command;
                }
                read(NOT);
                read(NULL);
                AlterTableAlterColumn command = new AlterTableAlterColumn(
                        session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumn(column);
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
                return command;
            } else if (readIf("TYPE")) {
                // PostgreSQL compatibility
                return parseAlterTableAlterColumnDataType(schema, tableName, columnName, ifTableExists);
            } else if (readIf("SET")) {
                if (readIf("DATA")) {
                    read("TYPE");
                    return parseAlterTableAlterColumnDataType(schema, tableName, columnName, ifTableExists);
                }
                AlterTableAlterColumn command = new AlterTableAlterColumn(
                        session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setOldColumn(column);
                NullConstraintType nullConstraint = parseNotNullConstraint();
                switch (nullConstraint) {
                case NULL_IS_ALLOWED:
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL);
                    break;
                case NULL_IS_NOT_ALLOWED:
                    command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL);
                    break;
                case NO_NULL_CONSTRAINT_FOUND:
                    if (readIf("DEFAULT")) {
                        Expression defaultExpression = readExpression();
                        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT);
                        command.setDefaultExpression(defaultExpression);
                    } else if (readIf(ON)) {
                        read("UPDATE");
                        Expression onUpdateExpression = readExpression();
                        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_ON_UPDATE);
                        command.setDefaultExpression(onUpdateExpression);
                    } else if (readIf("INVISIBLE")) {
                        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY);
                        command.setVisible(false);
                    } else if (readIf("VISIBLE")) {
                        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY);
                        command.setVisible(true);
                    }
                    break;
                default:
                    throw DbException.get(ErrorCode.UNKNOWN_MODE_1,
                            "Internal Error - unhandled case: " + nullConstraint.name());
                }
                return command;
            } else if (readIf("RESTART")) {
                readIf(WITH);
                Expression start = readExpression();
                AlterSequence command = new AlterSequence(session, schema);
                command.setColumn(column);
                SequenceOptions options = new SequenceOptions();
                options.setStartValue(start);
                command.setOptions(options);
                return commandIfTableExists(schema, tableName, ifTableExists, command);
            } else if (readIf("SELECTIVITY")) {
                AlterTableAlterColumn command = new AlterTableAlterColumn(
                        session, schema);
                command.setTableName(tableName);
                command.setIfTableExists(ifTableExists);
                command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY);
                command.setOldColumn(column);
                command.setSelectivity(readExpression());
                return command;
            } else {
                return parseAlterTableAlterColumnType(schema, tableName, columnName, ifTableExists);
            }
        }
        throw getSyntaxError();
    }

    private Table tableIfTableExists(Schema schema, String tableName, boolean ifTableExists) {
        Table table = schema.resolveTableOrView(session, tableName);
        if (table == null && !ifTableExists) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        return table;
    }

    private Column columnIfTableExists(Schema schema, String tableName,
            String columnName, boolean ifTableExists) {
        Table table = tableIfTableExists(schema, tableName, ifTableExists);
        return table == null ? null : table.getColumn(columnName);
    }

    private Prepared commandIfTableExists(Schema schema, String tableName,
            boolean ifTableExists, Prepared commandIfTableExists) {
        return tableIfTableExists(schema, tableName, ifTableExists) == null
            ? new NoOperation(session)
            : commandIfTableExists;
    }

    private AlterTableAlterColumn parseAlterTableAlterColumnType(Schema schema,
            String tableName, String columnName, boolean ifTableExists) {
        Column oldColumn = columnIfTableExists(schema, tableName, columnName, ifTableExists);
        Column newColumn = parseColumnForTable(columnName,
                oldColumn == null ? true : oldColumn.isNullable(), true);
        if (readIf(CHECK)) {
            Expression expr = readExpression();
            newColumn.addCheckConstraint(session, expr);
        }
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE);
        command.setOldColumn(oldColumn);
        command.setNewColumn(newColumn);
        return command;
    }

    private AlterTableAlterColumn parseAlterTableAlterColumnDataType(Schema schema,
            String tableName, String columnName, boolean ifTableExists) {
        Column oldColumn = columnIfTableExists(schema, tableName, columnName, ifTableExists);
        Column newColumn = parseColumnWithType(columnName, true);
        if (oldColumn != null) {
            if (!oldColumn.isNullable()) {
                newColumn.setNullable(false);
            }
            if (!oldColumn.getVisible()) {
                newColumn.setVisible(false);
            }
            Expression e = oldColumn.getDefaultExpression();
            if (e != null) {
                newColumn.setDefaultExpression(session, e);
            }
            e = oldColumn.getOnUpdateExpression();
            if (e != null) {
                newColumn.setOnUpdateExpression(session, e);
            }
            e = oldColumn.getCheckConstraint(session, columnName);
            if (e != null) {
                newColumn.addCheckConstraint(session, e);
            }
            String c = oldColumn.getComment();
            if (c != null) {
                newColumn.setComment(c);
            }
        }
        AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setType(CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE);
        command.setOldColumn(oldColumn);
        command.setNewColumn(newColumn);
        return command;
    }

    private AlterTableAlterColumn parseAlterTableAddColumn(String tableName,
            Schema schema, boolean ifTableExists) {
        readIf("COLUMN");
        AlterTableAlterColumn command = new AlterTableAlterColumn(session,
                schema);
        command.setType(CommandInterface.ALTER_TABLE_ADD_COLUMN);
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        if (readIf(OPEN_PAREN)) {
            command.setIfNotExists(false);
            do {
                parseTableColumnDefinition(command, schema, tableName, false);
            } while (readIfMore(true));
        } else {
            boolean ifNotExists = readIfNotExists();
            command.setIfNotExists(ifNotExists);
            parseTableColumnDefinition(command, schema, tableName, false);
        }
        if (readIf("BEFORE")) {
            command.setAddBefore(readColumnIdentifier());
        } else if (readIf("AFTER")) {
            command.setAddAfter(readColumnIdentifier());
        } else if (readIf("FIRST")) {
            command.setAddFirst();
        }
        return command;
    }

    private ConstraintActionType parseAction() {
        ConstraintActionType result = parseCascadeOrRestrict();
        if (result != null) {
            return result;
        }
        if (readIf("NO")) {
            read("ACTION");
            return ConstraintActionType.RESTRICT;
        }
        read("SET");
        if (readIf(NULL)) {
            return ConstraintActionType.SET_NULL;
        }
        read("DEFAULT");
        return ConstraintActionType.SET_DEFAULT;
    }

    private ConstraintActionType parseCascadeOrRestrict() {
        if (readIf("CASCADE")) {
            return ConstraintActionType.CASCADE;
        } else if (readIf("RESTRICT")) {
            return ConstraintActionType.RESTRICT;
        } else {
            return null;
        }
    }

    private DefineCommand parseAlterTableAddConstraintIf(String tableName,
            Schema schema, boolean ifTableExists) {
        String constraintName = null, comment = null;
        boolean ifNotExists = false;
        boolean allowIndexDefinition = database.getMode().indexDefinitionInCreateTable;
        boolean allowAffinityKey = database.getMode().allowAffinityKey;
        if (readIf(CONSTRAINT)) {
            ifNotExists = readIfNotExists();
            constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            comment = readCommentIf();
            allowIndexDefinition = true;
        }
        if (readIf(PRIMARY)) {
            read("KEY");
            AlterTableAddConstraint command = new AlterTableAddConstraint(
                    session, schema, ifNotExists);
            command.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY);
            command.setComment(comment);
            command.setConstraintName(constraintName);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            if (readIf("HASH")) {
                command.setPrimaryKeyHash(true);
            }
            read(OPEN_PAREN);
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            return command;
        } else if (allowIndexDefinition && (isToken("INDEX") || isToken("KEY"))) {
            // MySQL
            // need to read ahead, as it could be a column name
            int start = lastParseIndex;
            read();
            if (DataType.getTypeByName(currentToken, database.getMode()) != null) {
                // known data type
                parseIndex = start;
                read();
                return null;
            }
            CreateIndex command = new CreateIndex(session, schema);
            command.setComment(comment);
            command.setTableName(tableName);
            command.setIfTableExists(ifTableExists);
            if (!readIf(OPEN_PAREN)) {
                command.setIndexName(readUniqueIdentifier());
                read(OPEN_PAREN);
            }
            command.setIndexColumns(parseIndexColumnList());
            // MySQL compatibility
            if (readIf("USING")) {
                read("BTREE");
            }
            return command;
        } else if (allowAffinityKey && readIfAffinity()) {
            read("KEY");
            read(OPEN_PAREN);
            CreateIndex command = createAffinityIndex(schema, tableName, parseIndexColumnList());
            command.setIfTableExists(ifTableExists);
            return command;
        }
        AlterTableAddConstraint command;
        if (readIf(CHECK)) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK);
            command.setCheckExpression(readExpression());
        } else if (readIf(UNIQUE)) {
            readIf("KEY");
            readIf("INDEX");
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE);
            if (!readIf(OPEN_PAREN)) {
                constraintName = readUniqueIdentifier();
                read(OPEN_PAREN);
            }
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            // MySQL compatibility
            if (readIf("USING")) {
                read("BTREE");
            }
        } else if (readIf(FOREIGN)) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists);
            command.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL);
            read("KEY");
            read(OPEN_PAREN);
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                String indexName = readIdentifierWithSchema();
                command.setIndex(schema.findIndex(session, indexName));
            }
            read("REFERENCES");
            parseReferences(command, schema, tableName);
        } else {
            if (constraintName != null) {
                throw getSyntaxError();
            }
            return null;
        }
        if (readIf("NOCHECK")) {
            command.setCheckExisting(false);
        } else {
            readIf(CHECK);
            command.setCheckExisting(true);
        }
        command.setTableName(tableName);
        command.setIfTableExists(ifTableExists);
        command.setConstraintName(constraintName);
        command.setComment(comment);
        return command;
    }

    private void parseReferences(AlterTableAddConstraint command,
            Schema schema, String tableName) {
        if (readIf(OPEN_PAREN)) {
            command.setRefTableName(schema, tableName);
            command.setRefIndexColumns(parseIndexColumnList());
        } else {
            String refTableName = readIdentifierWithSchema(schema.getName());
            command.setRefTableName(getSchema(), refTableName);
            if (readIf(OPEN_PAREN)) {
                command.setRefIndexColumns(parseIndexColumnList());
            }
        }
        if (readIf("INDEX")) {
            String indexName = readIdentifierWithSchema();
            command.setRefIndex(getSchema().findIndex(session, indexName));
        }
        while (readIf(ON)) {
            if (readIf("DELETE")) {
                command.setDeleteAction(parseAction());
            } else {
                read("UPDATE");
                command.setUpdateAction(parseAction());
            }
        }
        if (readIf(NOT)) {
            read("DEFERRABLE");
        } else {
            readIf("DEFERRABLE");
        }
    }

    private CreateLinkedTable parseCreateLinkedTable(boolean temp,
            boolean globalTemp, boolean force) {
        read(TABLE);
        boolean ifNotExists = readIfNotExists();
        String tableName = readIdentifierWithSchema();
        CreateLinkedTable command = new CreateLinkedTable(session, getSchema());
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setForce(force);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        read(OPEN_PAREN);
        command.setDriver(readString());
        read(COMMA);
        command.setUrl(readString());
        read(COMMA);
        command.setUser(readString());
        read(COMMA);
        command.setPassword(readString());
        read(COMMA);
        String originalTable = readString();
        if (readIf(COMMA)) {
            command.setOriginalSchema(originalTable);
            originalTable = readString();
        }
        command.setOriginalTable(originalTable);
        read(CLOSE_PAREN);
        if (readIf("EMIT")) {
            read("UPDATES");
            command.setEmitUpdates(true);
        } else if (readIf("READONLY")) {
            command.setReadOnly(true);
        }
        return command;
    }

    private CreateTable parseCreateTable(boolean temp, boolean globalTemp,
            boolean persistIndexes) {
        boolean ifNotExists = readIfNotExists();
        String tableName = readIdentifierWithSchema();
        if (temp && globalTemp && equalsToken("SESSION", schemaName)) {
            // support weird syntax: declare global temporary table session.xy
            // (...) not logged
            schemaName = session.getCurrentSchemaName();
            globalTemp = false;
        }
        Schema schema = getSchema();
        CreateTable command = new CreateTable(session, schema);
        command.setPersistIndexes(persistIndexes);
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        if (readIf(OPEN_PAREN)) {
            if (!readIf(CLOSE_PAREN)) {
                do {
                    parseTableColumnDefinition(command, schema, tableName, true);
                } while (readIfMore(false));
            }
        }
        // Allows "COMMENT='comment'" in DDL statements (MySQL syntax)
        if (readIf("COMMENT")) {
            if (readIf(EQUAL)) {
                // read the complete string comment, but nothing with it for now
                readString();
            }
        }
        if (readIf("ENGINE")) {
            if (readIf(EQUAL)) {
                // map MySQL engine types onto H2 behavior
                String tableEngine = readUniqueIdentifier();
                if ("InnoDb".equalsIgnoreCase(tableEngine)) {
                    // ok
                } else if (!"MyISAM".equalsIgnoreCase(tableEngine)) {
                    throw DbException.getUnsupportedException(tableEngine);
                }
            } else {
                command.setTableEngine(readUniqueIdentifier());
            }
        }
        if (readIf(WITH)) {
            command.setTableEngineParams(readTableEngineParams());
        }
        // MySQL compatibility
        if (readIf("AUTO_INCREMENT")) {
            read(EQUAL);
            if (currentTokenType != VALUE ||
                    currentValue.getValueType() != Value.INT) {
                throw DbException.getSyntaxError(sqlCommand, parseIndex,
                        "integer");
            }
            read();
        }
        readIf("DEFAULT");
        if (readIf("CHARSET")) {
            read(EQUAL);
            if (!readIf("UTF8")) {
                read("UTF8MB4");
            }
        }
        if (temp) {
            if (readIf(ON)) {
                read("COMMIT");
                if (readIf("DROP")) {
                    command.setOnCommitDrop();
                } else if (readIf("DELETE")) {
                    read("ROWS");
                    command.setOnCommitTruncate();
                }
            } else if (readIf(NOT)) {
                if (readIf("PERSISTENT")) {
                    command.setPersistData(false);
                } else {
                    read("LOGGED");
                }
            }
            if (readIf("TRANSACTIONAL")) {
                command.setTransactional(true);
            }
        } else if (!persistIndexes && readIf(NOT)) {
            read("PERSISTENT");
            command.setPersistData(false);
        }
        if (readIf("HIDDEN")) {
            command.setHidden(true);
        }
        if (readIf("AS")) {
            if (readIf("SORTED")) {
                command.setSortedInsertMode(true);
            }
            command.setQuery(parseSelect());
            if (readIf(WITH)) {
                command.setWithNoData(readIf("NO"));
                read("DATA");
            }
        }
        // for MySQL compatibility
        if (readIf("ROW_FORMAT")) {
            if (readIf(EQUAL)) {
                readColumnIdentifier();
            }
        }
        return command;
    }

    private void parseTableColumnDefinition(CommandWithColumns command, Schema schema, String tableName,
            boolean forCreateTable) {
        DefineCommand c = parseAlterTableAddConstraintIf(tableName, schema, false);
        if (c != null) {
            command.addConstraintCommand(c);
        } else {
            String columnName = readColumnIdentifier();
            if (forCreateTable && (currentTokenType == COMMA || currentTokenType == CLOSE_PAREN)) {
                command.addColumn(new Column(columnName, TypeInfo.TYPE_UNKNOWN));
                return;
            }
            Column column = parseColumnForTable(columnName, true, true);
            if (column.isAutoIncrement() && column.isPrimaryKey()) {
                column.setPrimaryKey(false);
                IndexColumn[] cols = { new IndexColumn() };
                cols[0].columnName = column.getName();
                AlterTableAddConstraint pk = new AlterTableAddConstraint(
                        session, schema, false);
                pk.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY);
                pk.setTableName(tableName);
                pk.setIndexColumns(cols);
                command.addConstraintCommand(pk);
            }
            command.addColumn(column);
            String constraintName = null;
            if (readIf(CONSTRAINT)) {
                constraintName = readColumnIdentifier();
            }
            // For compatibility with Apache Ignite.
            boolean allowAffinityKey = database.getMode().allowAffinityKey;
            boolean affinity = allowAffinityKey && readIfAffinity();
            if (readIf(PRIMARY)) {
                read("KEY");
                boolean hash = readIf("HASH");
                IndexColumn[] cols = { new IndexColumn() };
                cols[0].columnName = column.getName();
                AlterTableAddConstraint pk = new AlterTableAddConstraint(
                        session, schema, false);
                pk.setConstraintName(constraintName);
                pk.setPrimaryKeyHash(hash);
                pk.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY);
                pk.setTableName(tableName);
                pk.setIndexColumns(cols);
                command.addConstraintCommand(pk);
                if (readIf("AUTO_INCREMENT")) {
                    parseAutoIncrement(column);
                }
                if (database.getMode().useIdentityAsAutoIncrement) {
                    if (readIf(NOT)) {
                        read(NULL);
                        column.setNullable(false);
                    }
                    if (readIf("IDENTITY")) {
                        parseAutoIncrement(column);
                    }
                }
                if (affinity) {
                    CreateIndex idx = createAffinityIndex(schema, tableName, cols);
                    command.addConstraintCommand(idx);
                }
            } else if (affinity) {
                read("KEY");
                IndexColumn[] cols = { new IndexColumn() };
                cols[0].columnName = column.getName();
                CreateIndex idx = createAffinityIndex(schema, tableName, cols);
                command.addConstraintCommand(idx);
            } else if (readIf(UNIQUE)) {
                AlterTableAddConstraint unique = new AlterTableAddConstraint(
                        session, schema, false);
                unique.setConstraintName(constraintName);
                unique.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE);
                IndexColumn[] cols = { new IndexColumn() };
                cols[0].columnName = columnName;
                unique.setIndexColumns(cols);
                unique.setTableName(tableName);
                command.addConstraintCommand(unique);
            }
            if (NullConstraintType.NULL_IS_NOT_ALLOWED == parseNotNullConstraint()) {
                column.setNullable(false);
            }
            if (readIf(CHECK)) {
                Expression expr = readExpression();
                column.addCheckConstraint(session, expr);
            }
            if (readIf("REFERENCES")) {
                AlterTableAddConstraint ref = new AlterTableAddConstraint(
                        session, schema, false);
                ref.setConstraintName(constraintName);
                ref.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL);
                IndexColumn[] cols = { new IndexColumn() };
                cols[0].columnName = columnName;
                ref.setIndexColumns(cols);
                ref.setTableName(tableName);
                parseReferences(ref, schema, tableName);
                command.addConstraintCommand(ref);
            }
        }
    }

    /**
     * Enumeration describing null constraints
     */
    private enum NullConstraintType {
        NULL_IS_ALLOWED, NULL_IS_NOT_ALLOWED, NO_NULL_CONSTRAINT_FOUND
    }

    private NullConstraintType parseNotNullConstraint() {
        NullConstraintType nullConstraint = NullConstraintType.NO_NULL_CONSTRAINT_FOUND;
        if (isToken(NOT) || isToken(NULL)) {
            if (readIf(NOT)) {
                read(NULL);
                nullConstraint = NullConstraintType.NULL_IS_NOT_ALLOWED;
            } else {
                read(NULL);
                nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
            }
            if (database.getMode().getEnum() == ModeEnum.Oracle) {
                if (readIf("ENABLE")) {
                    // Leave constraint 'as is'
                    readIf("VALIDATE");
                    // Turn off constraint, allow NULLs
                    if (readIf("NOVALIDATE")) {
                        nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
                    }
                }
                // Turn off constraint, allow NULLs
                if (readIf("DISABLE")) {
                    nullConstraint = NullConstraintType.NULL_IS_ALLOWED;
                    // ignore validate
                    readIf("VALIDATE");
                    // ignore novalidate
                    readIf("NOVALIDATE");
                }
            }
        }
        return nullConstraint;
    }

    private CreateSynonym parseCreateSynonym(boolean orReplace) {
        boolean ifNotExists = readIfNotExists();
        String name = readIdentifierWithSchema();
        Schema synonymSchema = getSchema();
        read(FOR);
        String tableName = readIdentifierWithSchema();

        Schema targetSchema = getSchema();
        CreateSynonym command = new CreateSynonym(session, synonymSchema);
        command.setName(name);
        command.setSynonymFor(tableName);
        command.setSynonymForSchema(targetSchema);
        command.setComment(readCommentIf());
        command.setIfNotExists(ifNotExists);
        command.setOrReplace(orReplace);
        return command;
    }

    private CreateIndex createAffinityIndex(Schema schema, String tableName, IndexColumn[] indexColumns) {
        CreateIndex idx = new CreateIndex(session, schema);
        idx.setTableName(tableName);
        idx.setIndexColumns(indexColumns);
        idx.setAffinity(true);
        return idx;
    }

    private static int getCompareType(int tokenType) {
        switch (tokenType) {
        case EQUAL:
            return Comparison.EQUAL;
        case BIGGER_EQUAL:
            return Comparison.BIGGER_EQUAL;
        case BIGGER:
            return Comparison.BIGGER;
        case SMALLER:
            return Comparison.SMALLER;
        case SMALLER_EQUAL:
            return Comparison.SMALLER_EQUAL;
        case NOT_EQUAL:
            return Comparison.NOT_EQUAL;
        case SPATIAL_INTERSECTS:
            return Comparison.SPATIAL_INTERSECTS;
        default:
            return -1;
        }
    }

    /**
     * Add double quotes around an identifier if required.
     *
     * @param s the identifier
     * @param alwaysQuote quote all identifiers
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String s, boolean alwaysQuote) {
        if (s == null) {
            return "\"\"";
        }
        if (!alwaysQuote && ParserUtil.isSimpleIdentifier(s, false, false)) {
            return s;
        }
        return StringUtils.quoteIdentifier(s);
    }

    /**
     * Add double quotes around an identifier if required and appends it to the
     * specified string builder.
     *
     * @param builder string builder to append to
     * @param s the identifier
     * @param alwaysQuote quote all identifiers
     * @return the specified builder
     */
    public static StringBuilder quoteIdentifier(StringBuilder builder, String s, boolean alwaysQuote) {
        if (s == null) {
            return builder.append("\"\"");
        }
        if (!alwaysQuote && ParserUtil.isSimpleIdentifier(s, false, false)) {
            return builder.append(s);
        }
        return StringUtils.quoteIdentifier(builder, s);
    }

    public void setLiteralsChecked(boolean literalsChecked) {
        this.literalsChecked = literalsChecked;
    }

    public void setRightsChecked(boolean rightsChecked) {
        this.rightsChecked = rightsChecked;
    }

    public void setSuppliedParameterList(ArrayList<Parameter> suppliedParameterList) {
        this.suppliedParameterList = suppliedParameterList;
    }

    /**
     * Parse a SQL code snippet that represents an expression.
     *
     * @param sql the code snippet
     * @return the expression object
     */
    public Expression parseExpression(String sql) {
        parameters = Utils.newSmallArrayList();
        initialize(sql);
        read();
        return readExpression();
    }

    /**
     * Parse a SQL code snippet that represents a table name.
     *
     * @param sql the code snippet
     * @return the table object
     */
    public Table parseTableName(String sql) {
        parameters = Utils.newSmallArrayList();
        initialize(sql);
        read();
        return readTableOrView();
    }

    @Override
    public String toString() {
        return StringUtils.addAsterisk(sqlCommand, parseIndex);
    }
}
