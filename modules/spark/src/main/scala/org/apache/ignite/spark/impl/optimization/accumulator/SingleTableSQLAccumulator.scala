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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Class for accumulating parts of SQL query to a single Ignite table.
  *
  * See <a href="http://www.h2database.com/html/grammar.html#select">select syntax of H2</a>.
  */
private[apache] case class SingleTableSQLAccumulator(
    igniteQueryContext: IgniteQueryContext,
    table: Option[String],
    tableExpression: Option[(QueryAccumulator, String)],
    outputExpressions: Seq[NamedExpression],
    distinct: Boolean = false,
    all: Boolean = false,
    where: Option[Seq[Expression]] = None,
    groupBy: Option[Seq[Expression]] = None,
    having: Option[Seq[Expression]] = None,
    limit: Option[Expression] = None,
    localLimit: Option[Expression] = None,
    orderBy: Option[Seq[SortOrder]] = None
) extends SelectAccumulator {
    /** @inheritdoc */
    override def compileQuery(prettyPrint: Boolean = false, nestedQuery: Boolean = false): String = {
        val delim = if (prettyPrint) "\n" else " "
        val tab = if (prettyPrint) "  " else ""

        var sql = s"SELECT$delim$tab${outputExpressions.map(exprToString(_)).mkString(", ")}${delim}" +
            s"FROM$delim$tab$compiledTableExpression"

        if (where.exists(_.nonEmpty))
            sql += s"${delim}WHERE$delim$tab${where.get.map(exprToString(_)).mkString(s" AND$delim$tab")}"

        if (groupBy.exists(_.nonEmpty))
            sql += s"${delim}GROUP BY ${groupBy.get.map(exprToString(_)).mkString(s",$delim$tab")}"

        if (having.exists(_.nonEmpty))
            sql += s"${delim}HAVING ${having.get.map(exprToString(_)).mkString(s" AND$delim$tab")}"

        if (orderBy.exists(_.nonEmpty))
            sql += s"${delim}ORDER BY ${orderBy.get.map(exprToString(_)).mkString(s",$delim$tab")}"

        if (limit.isDefined) {
            sql += s" LIMIT ${limit.map(exprToString(_)).get}"

            if (nestedQuery)
                sql = s"SELECT * FROM ($sql)"
        }

        sql
    }

    /**
      * @return From table SQL query part.
      */
    private def compiledTableExpression: String = table match {
        case Some(tableName) ⇒
            tableName

        case None ⇒ tableExpression match {
            case Some((acc, alias)) ⇒
                s"(${acc.compileQuery()}) $alias"

            case None ⇒
                throw new IgniteException("Unknown table.")
        }
    }

    /** @inheritdoc */
    override def simpleString: String =
        s"IgniteSQLAccumulator(table: $table, columns: $outputExpressions, distinct: $distinct, all: $all, " +
            s"where: $where, groupBy: $groupBy, having: $having, limit: $limit, orderBy: $orderBy)"

    /** @inheritdoc */
    override def withOutputExpressions(outputExpressions: Seq[NamedExpression]): SelectAccumulator =
        copy(outputExpressions= outputExpressions)

    /** @inheritdoc */
    override def withDistinct(distinct: Boolean): SingleTableSQLAccumulator = copy(distinct = distinct)

    /** @inheritdoc */
    override def withWhere(where: Seq[Expression]): SingleTableSQLAccumulator = copy(where = Some(where))

    /** @inheritdoc */
    override def withGroupBy(groupBy: Seq[Expression]): SingleTableSQLAccumulator = copy(groupBy = Some(groupBy))

    /** @inheritdoc */
    override def withHaving(having: Seq[Expression]): SingleTableSQLAccumulator = copy(having = Some(having))

    /** @inheritdoc */
    override def withLimit(limit: Expression): SingleTableSQLAccumulator = copy(limit = Some(limit))

    /** @inheritdoc */
    override def withLocalLimit(localLimit: Expression): SingleTableSQLAccumulator = copy(localLimit = Some(localLimit))

    /** @inheritdoc */
    override def withOrderBy(orderBy: Seq[SortOrder]): SingleTableSQLAccumulator = copy(orderBy = Some(orderBy))

    /** @inheritdoc */
    override def output: Seq[Attribute] = outputExpressions.map(toAttributeReference(_, Seq.empty))

    /** @inheritdoc */
    override def qualifier: String = table.getOrElse(tableExpression.get._2)

    /** @inheritdoc */
    override def children: Seq[LogicalPlan] = tableExpression.map(te ⇒ Seq(te._1)).getOrElse(Nil)
}
