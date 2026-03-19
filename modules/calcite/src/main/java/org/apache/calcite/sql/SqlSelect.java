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
package org.apache.calcite.sql;

import java.util.List;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlSelect extends SqlCall {
    //~ Static fields/initializers ---------------------------------------------

    /** Operand index of {@code FROM} clause. */
    public static final int FROM_OPERAND = 2;

    /** Operand index of {@code WHERE} clause. */
    public static final int WHERE_OPERAND = 3;

    /** Operand index of {@code HAVING} clause. */
    public static final int HAVING_OPERAND = 5;

    /** Operand index of {@code QUALIFY} clause. */
    public static final int QUALIFY_OPERAND = 7;

    /** Select modifiers, for example DISTINCT. */
    SqlNodeList keywordList;

    /** Projection list. */
    SqlNodeList selectList;

    /** Source relation. */
    @Nullable SqlNode from;

    /** Filter condition. */
    @Nullable SqlNode where;

    /** Grouping expressions. */
    @Nullable SqlNodeList groupBy;

    /** HAVING condition. */
    @Nullable SqlNode having;

    /** Window declarations. */
    SqlNodeList windowDecls;

    /** QUALIFY condition. */
    @Nullable SqlNode qualify;

    /** ORDER BY list. */
    @Nullable SqlNodeList orderBy;

    /** OFFSET expression. */
    @Nullable SqlNode offset;

    /** FETCH/LIMIT expression. */
    @Nullable SqlNode fetch;

    /** Query hints. */
    @Nullable SqlNodeList hints;

    /** Whether query has explicit {@code FOR UPDATE} clause. */
    boolean forUpdate;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates select node.
     *
     * @param pos Parser position.
     * @param keywordList Select modifiers.
     * @param selectList Projection list.
     * @param from Source relation.
     * @param where Filter condition.
     * @param groupBy Grouping expressions.
     * @param having HAVING condition.
     * @param windowDecls Window declarations.
     * @param qualify QUALIFY condition.
     * @param orderBy ORDER BY list.
     * @param offset OFFSET expression.
     * @param fetch FETCH/LIMIT expression.
     * @param hints Query hints.
     */
    public SqlSelect(SqlParserPos pos,
        @Nullable SqlNodeList keywordList,
        SqlNodeList selectList,
        @Nullable SqlNode from,
        @Nullable SqlNode where,
        @Nullable SqlNodeList groupBy,
        @Nullable SqlNode having,
        @Nullable SqlNodeList windowDecls,
        @Nullable SqlNode qualify,
        @Nullable SqlNodeList orderBy,
        @Nullable SqlNode offset,
        @Nullable SqlNode fetch,
        @Nullable SqlNodeList hints) {
        super(pos);
        this.keywordList = requireNonNull(keywordList != null
            ? keywordList : new SqlNodeList(pos));
        this.selectList = requireNonNull(selectList, "selectList");
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windowDecls = requireNonNull(windowDecls != null
            ? windowDecls : new SqlNodeList(pos));
        this.qualify = qualify;
        this.orderBy = orderBy;
        this.offset = offset;
        this.fetch = fetch;
        this.hints = hints;
    }

    /**
     * Creates select node without {@code QUALIFY} clause.
     *
     * @param pos Parser position.
     * @param keywordList Select modifiers.
     * @param selectList Projection list.
     * @param from Source relation.
     * @param where Filter condition.
     * @param groupBy Grouping expressions.
     * @param having HAVING condition.
     * @param windowDecls Window declarations.
     * @param orderBy ORDER BY list.
     * @param offset OFFSET expression.
     * @param fetch FETCH/LIMIT expression.
     * @param hints Query hints.
     * @deprecated to be removed before 2.0.
     */
    @Deprecated // to be removed before 2.0
    public SqlSelect(SqlParserPos pos,
        @Nullable SqlNodeList keywordList,
        SqlNodeList selectList,
        @Nullable SqlNode from,
        @Nullable SqlNode where,
        @Nullable SqlNodeList groupBy,
        @Nullable SqlNode having,
        @Nullable SqlNodeList windowDecls,
        @Nullable SqlNodeList orderBy,
        @Nullable SqlNode offset,
        @Nullable SqlNode fetch,
        @Nullable SqlNodeList hints) {
        this(pos, keywordList, selectList, from, where, groupBy, having,
            windowDecls, null, orderBy, offset, fetch, hints);
    }

    //~ Methods ----------------------------------------------------------------

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return SqlSelectOperator.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public SqlKind getKind() {
        return SqlKind.SELECT;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(keywordList, selectList, from, where,
            groupBy, having, windowDecls, qualify, orderBy, offset, fetch, hints);
    }

    /** {@inheritDoc} */
    @Override public void setOperand(int i, @Nullable SqlNode operand) {
        switch (i) {
            case 0:
                keywordList = requireNonNull((SqlNodeList)operand);
                break;
            case 1:
                selectList = requireNonNull((SqlNodeList)operand);
                break;
            case 2:
                from = operand;
                break;
            case 3:
                where = operand;
                break;
            case 4:
                groupBy = (SqlNodeList)operand;
                break;
            case 5:
                having = operand;
                break;
            case 6:
                windowDecls = requireNonNull((SqlNodeList)operand);
                break;
            case 7:
                qualify = operand;
                break;
            case 8:
                orderBy = (SqlNodeList)operand;
                break;
            case 9:
                offset = operand;
                break;
            case 10:
                fetch = operand;
                break;
            default:
                throw new AssertionError(i);
        }
    }

    /**
     * Returns whether {@code DISTINCT} modifier is present.
     *
     * @return {@code true} if DISTINCT is present.
     */
    public final boolean isDistinct() {
        return getModifierNode(SqlSelectKeyword.DISTINCT) != null;
    }

    /**
     * Returns a node for the given modifier.
     *
     * @param modifier Select modifier.
     * @return Modifier node if present, otherwise {@code null}.
     */
    public final @Nullable SqlNode getModifierNode(SqlSelectKeyword modifier) {
        for (SqlNode keyword : keywordList) {
            SqlSelectKeyword keyword2 =
                ((SqlLiteral)keyword).symbolValue(SqlSelectKeyword.class);
            if (keyword2 == modifier) {
                return keyword;
            }
        }
        return null;
    }

    /**
     * Returns source relation.
     *
     * @return Source relation.
     */
    @Pure
    public final @Nullable SqlNode getFrom() {
        return from;
    }

    /**
     * Sets source relation.
     *
     * @param from Source relation.
     */
    public void setFrom(@Nullable SqlNode from) {
        this.from = from;
    }

    /**
     * Returns grouping expressions.
     *
     * @return Grouping expressions.
     */
    @Pure
    public final @Nullable SqlNodeList getGroup() {
        return groupBy;
    }

    /**
     * Sets grouping expressions.
     *
     * @param groupBy Grouping expressions.
     */
    public void setGroupBy(@Nullable SqlNodeList groupBy) {
        this.groupBy = groupBy;
    }

    /**
     * Returns HAVING condition.
     *
     * @return HAVING condition.
     */
    @Pure
    public final @Nullable SqlNode getHaving() {
        return having;
    }

    /**
     * Sets HAVING condition.
     *
     * @param having HAVING condition.
     */
    public void setHaving(@Nullable SqlNode having) {
        this.having = having;
    }

    /**
     * Returns projection list.
     *
     * @return Projection list.
     */
    @Pure
    public final SqlNodeList getSelectList() {
        return selectList;
    }

    /**
     * Sets projection list.
     *
     * @param selectList Projection list.
     */
    public void setSelectList(SqlNodeList selectList) {
        this.selectList = selectList;
    }

    /**
     * Returns filter condition.
     *
     * @return Filter condition.
     */
    @Pure
    public final @Nullable SqlNode getWhere() {
        return where;
    }

    /**
     * Sets filter condition.
     *
     * @param whereClause Filter condition.
     */
    public void setWhere(@Nullable SqlNode whereClause) {
        this.where = whereClause;
    }

    /**
     * Returns window declarations.
     *
     * @return Window declarations.
     */
    public final SqlNodeList getWindowList() {
        return windowDecls;
    }

    /**
     * Returns QUALIFY condition.
     *
     * @return QUALIFY condition.
     */
    @Pure
    public final @Nullable SqlNode getQualify() {
        return qualify;
    }

    /**
     * Sets QUALIFY condition.
     *
     * @param qualify QUALIFY condition.
     */
    public void setQualify(@Nullable SqlNode qualify) {
        this.qualify = qualify;
    }

    /**
     * Returns ORDER BY list.
     *
     * @return ORDER BY list.
     */
    @Pure
    public final @Nullable SqlNodeList getOrderList() {
        return orderBy;
    }

    /**
     * Sets ORDER BY list.
     *
     * @param orderBy ORDER BY list.
     */
    public void setOrderBy(@Nullable SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    /**
     * Returns OFFSET expression.
     *
     * @return OFFSET expression.
     */
    @Pure
    public final @Nullable SqlNode getOffset() {
        return offset;
    }

    /**
     * Sets OFFSET expression.
     *
     * @param offset OFFSET expression.
     */
    public void setOffset(@Nullable SqlNode offset) {
        this.offset = offset;
    }

    /**
     * Returns FETCH/LIMIT expression.
     *
     * @return FETCH/LIMIT expression.
     */
    @Pure
    public final @Nullable SqlNode getFetch() {
        return fetch;
    }

    /**
     * Sets FETCH/LIMIT expression.
     *
     * @param fetch FETCH/LIMIT expression.
     */
    public void setFetch(@Nullable SqlNode fetch) {
        this.fetch = fetch;
    }

    /**
     * Sets query hints.
     *
     * @param hints Query hints.
     */
    public void setHints(@Nullable SqlNodeList hints) {
        this.hints = hints;
    }

    /**
     * Returns query hints.
     *
     * @return Query hints.
     */
    @Pure
    public @Nullable SqlNodeList getHints() {
        return hints;
    }

    /**
     * Returns whether this select has an explicit {@code FOR UPDATE} clause.
     *
     * @return {@code true} if this query contains {@code FOR UPDATE}.
     */
    public boolean isForUpdate() {
        return forUpdate;
    }

    /**
     * Sets {@code FOR UPDATE} flag parsed from SQL text.
     *
     * @param forUpdate {@code true} if {@code FOR UPDATE} was present in the query text.
     */
    public void setForUpdate(boolean forUpdate) {
        this.forUpdate = forUpdate;
    }

    /**
     * Returns whether query has at least one hint.
     *
     * @return {@code true} if query contains hints.
     */
    @EnsuresNonNullIf(expression = "hints", result = true)
    public boolean hasHints() {
        // The hints may be passed as null explicitly.
        return hints != null && !hints.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!writer.inQuery()
            || getFetch() != null
            && (leftPrec > SqlInternalOperators.FETCH.getLeftPrec()
            || rightPrec > SqlInternalOperators.FETCH.getLeftPrec())
            || getOffset() != null
            && (leftPrec > SqlInternalOperators.OFFSET.getLeftPrec()
            || rightPrec > SqlInternalOperators.OFFSET.getLeftPrec())
            || getOrderList() != null
            && (leftPrec > SqlOrderBy.OPERATOR.getLeftPrec()
            || rightPrec > SqlOrderBy.OPERATOR.getRightPrec())) {
            // If this SELECT is the topmost item in a sub-query, introduce a new
            // frame. (The topmost item in the sub-query might be a UNION or
            // ORDER. In this case, we don't need a wrapper frame.)
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
            writer.getDialect().unparseCall(writer, this, 0, 0);
            writer.endList(frame);
        }
        else {
            writer.getDialect().unparseCall(writer, this, leftPrec, rightPrec);
        }
    }

    /**
     * Returns whether ORDER BY clause is present.
     *
     * @return {@code true} if ORDER BY clause is present.
     */
    public boolean hasOrderBy() {
        return orderBy != null && !orderBy.isEmpty();
    }

    /**
     * Returns whether WHERE clause is present.
     *
     * @return {@code true} if WHERE clause is present.
     */
    public boolean hasWhere() {
        return where != null;
    }

    /**
     * Returns whether the given select modifier is present.
     *
     * @param targetKeyWord Select modifier.
     * @return {@code true} if the modifier is present.
     */
    public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
        return getModifierNode(targetKeyWord) != null;
    }
}
