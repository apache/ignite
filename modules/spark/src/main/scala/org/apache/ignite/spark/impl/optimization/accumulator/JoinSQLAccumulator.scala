/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spark.impl.optimization.accumulator

import org.apache.ignite.IgniteException
import org.apache.ignite.spark.impl.optimization._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}

/**
  * Accumulator to store information about join query.
  */
private[apache] case class JoinSQLAccumulator(
    igniteQueryContext: IgniteQueryContext,
    left: QueryAccumulator,
    right: QueryAccumulator,
    joinType: JoinType,
    outputExpressions: Seq[NamedExpression],
    condition: Option[Expression],
    leftAlias: Option[String],
    rightAlias: Option[String],
    distinct: Boolean = false,
    where: Option[Seq[Expression]] = None,
    groupBy: Option[Seq[Expression]] = None,
    having: Option[Seq[Expression]] = None,
    limit: Option[Expression] = None,
    localLimit: Option[Expression] = None,
    orderBy: Option[Seq[SortOrder]] = None
) extends BinaryNode with SelectAccumulator {
    /** @inheritdoc */
    override def compileQuery(prettyPrint: Boolean = false, nestedQuery: Boolean = false): String = {
        val delim = if (prettyPrint) "\n" else " "
        val tab = if (prettyPrint) "  " else ""

        var sql = s"SELECT$delim$tab" +
            s"${fixQualifier(outputExpressions).map(exprToString(_, useQualifier = true)).mkString(", ")}$delim" +
            s"FROM$delim$tab$compileJoinExpr"

        if (allFilters.nonEmpty)
            sql += s"${delim}WHERE$delim$tab" +
                s"${fixQualifier(allFilters).map(exprToString(_, useQualifier = true)).mkString(s" AND$delim$tab")}"

        if (groupBy.exists(_.nonEmpty))
            sql += s"${delim}GROUP BY " +
                s"${fixQualifier(groupBy.get).map(exprToString(_, useQualifier = true)).mkString(s",$delim$tab")}"

        if (having.exists(_.nonEmpty))
            sql += s"${delim}HAVING " +
                s"${fixQualifier(having.get).map(exprToString(_, useQualifier = true)).mkString(s" AND$delim$tab")}"

        if (orderBy.exists(_.nonEmpty))
            sql += s"${delim}ORDER BY " +
                s"${fixQualifier(orderBy.get).map(exprToString(_, useQualifier = true)).mkString(s",$delim$tab")}"

        if (limit.isDefined) {
            sql += s" LIMIT ${exprToString(fixQualifier0(limit.get), useQualifier = true)}"

            if (nestedQuery)
                sql = s"SELECT * FROM ($sql)"
        }

        sql
    }

    /**
      * @return Filters for this query.
      */
    private def allFilters: Seq[Expression] = {
        val leftFilters =
            if (isSimpleTableAcc(left))
                left.asInstanceOf[SingleTableSQLAccumulator].where.getOrElse(Seq.empty)
            else
                Seq.empty

        val rightFilters =
            if (isSimpleTableAcc(right))
                right.asInstanceOf[SingleTableSQLAccumulator].where.getOrElse(Seq.empty)
            else Seq.empty

        where.getOrElse(Seq.empty) ++ leftFilters ++ rightFilters
    }

    /**
      * @return `table1 LEFT JOIN (SELECT....FROM...) table2` part of join query.
      */
    private def compileJoinExpr: String = {
        val leftJoinSql =
            if (isSimpleTableAcc(left))
                left.asInstanceOf[SingleTableSQLAccumulator].table.get
            else
                s"(${left.compileQuery()}) ${leftAlias.get}"

        val rightJoinSql = {
            val leftTableName =
                if (isSimpleTableAcc(left))
                    left.qualifier
                else
                    leftAlias.get

            if (isSimpleTableAcc(right)) {
                val rightTableName = right.asInstanceOf[SingleTableSQLAccumulator].table.get

                if (leftTableName == rightTableName)
                    s"$rightTableName as ${rightAlias.get}"
                else
                    rightTableName
            } else
                s"(${right.compileQuery()}) ${rightAlias.get}"
        }

        s"$leftJoinSql $joinTypeSQL $rightJoinSql" +
            s"${condition.map(expr ⇒ s" ON ${exprToString(fixQualifier0(expr), useQualifier = true)}").getOrElse("")}"
    }

    /**
      * @return SQL string representing specific join type.
      */
    private def joinTypeSQL = joinType match {
        case Inner ⇒
            "JOIN"
        case LeftOuter ⇒
            "LEFT JOIN"

        case RightOuter ⇒
            "RIGHT JOIN"

        case _ ⇒
            throw new IgniteException(s"Unsupported join type $joinType")
    }

    /**
      * Changes table qualifier in case of embedded query.
      *
      * @param exprs Expressions to fix.
      * @tparam T type of input expression.
      * @return copy of `exprs` with fixed qualifier.
      */
    private def fixQualifier[T <: Expression](exprs: Seq[T]): Seq[T] =
        exprs.map(fixQualifier0)

    /**
      * Changes table qualifier for single expression.
      *
      * @param expr Expression to fix.
      * @tparam T type of input expression.
      * @return copy of `expr` with fixed qualifier.
      */
    private def fixQualifier0[T <: Expression](expr: T): T = expr match {
        case attr: AttributeReference ⇒
            attr.withQualifier(Some(findQualifier(attr))).asInstanceOf[T]

        case _ ⇒
            expr.withNewChildren(fixQualifier(expr.children)).asInstanceOf[T]
    }

    /**
      * Find right qualifier for a `attr`.
      *
      * @param attr Attribute to fix qualifier in
      * @return Right qualifier for a `attr`
      */
    private def findQualifier(attr: AttributeReference): String = {
        val leftTableName =
            if (isSimpleTableAcc(left))
                left.qualifier
            else
                leftAlias.get

        if (left.outputExpressions.exists(_.exprId == attr.exprId))
            leftTableName
        else if (isSimpleTableAcc(right) && right.qualifier != leftTableName)
            right.qualifier
        else
            rightAlias.get
    }

    /** @inheritdoc */
    override def simpleString: String =
        s"JoinSQLAccumulator(joinType: $joinType, columns: $outputExpressions, condition: $condition)"

    /** @inheritdoc */
    override def withOutputExpressions(outputExpressions: Seq[NamedExpression]): SelectAccumulator = copy(outputExpressions= outputExpressions)

    /** @inheritdoc */
    override def withDistinct(distinct: Boolean): JoinSQLAccumulator = copy(distinct = distinct)

    /** @inheritdoc */
    override def withWhere(where: Seq[Expression]): JoinSQLAccumulator = copy(where = Some(where))

    /** @inheritdoc */
    override def withGroupBy(groupBy: Seq[Expression]): JoinSQLAccumulator = copy(groupBy = Some(groupBy))

    /** @inheritdoc */
    override def withHaving(having: Seq[Expression]): JoinSQLAccumulator = copy(having = Some(having))

    /** @inheritdoc */
    override def withLimit(limit: Expression): JoinSQLAccumulator = copy(limit = Some(limit))

    /** @inheritdoc */
    override def withLocalLimit(localLimit: Expression): JoinSQLAccumulator = copy(localLimit = Some(localLimit))

    /** @inheritdoc */
    override def withOrderBy(orderBy: Seq[SortOrder]): JoinSQLAccumulator = copy(orderBy = Some(orderBy))

    /** @inheritdoc */
    override def output: Seq[Attribute] = outputExpressions.map(toAttributeReference(_, Seq.empty))

    /** @inheritdoc */
    override lazy val qualifier: String = igniteQueryContext.uniqueTableAlias
}
