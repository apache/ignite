/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark.impl.optimization.accumulator

import org.apache.ignite.spark.impl.optimization.{IgniteQueryContext, exprToString, toAttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}

/**
  * Accumulator to store info about UNION query.
  */
private[apache] case class UnionSQLAccumulator(
    igniteQueryContext: IgniteQueryContext,
    children: Seq[QueryAccumulator],
    outputExpressions: Seq[NamedExpression],
    limit: Option[Expression] = None,
    localLimit: Option[Expression] = None,
    orderBy: Option[Seq[SortOrder]] = None
) extends QueryAccumulator {
    /** @inheritdoc */
    override def compileQuery(prettyPrint: Boolean = false, nestedQuery: Boolean = false): String = {
        val delim = if (prettyPrint) "\n" else " "
        val tab = if (prettyPrint) "  " else ""

        var query = children.map(_.compileQuery(prettyPrint, nestedQuery = true)).mkString(s"${delim}UNION$delim")

        query = orderBy match {
            case Some(sortOrders) ⇒
                query + s"${delim}ORDER BY ${sortOrders.map(exprToString(_)).mkString(s",$delim$tab")}"

            case None ⇒ query
        }

        if (limit.isDefined) {
            query += s" LIMIT ${exprToString(limit.get)}"

            if (nestedQuery)
                query = s"SELECT * FROM ($query)"
        }

        query
    }

    /** @inheritdoc */
    override def simpleString: String =
        s"UnionSQLAccumulator(orderBy: ${orderBy.map(_.map(exprToString(_)).mkString(", ")).getOrElse("[]")})"

    /** @inheritdoc */
    override def withOutputExpressions(outputExpressions: Seq[NamedExpression]): QueryAccumulator =
        copy(outputExpressions= outputExpressions)

    /** @inheritdoc */
    override def withOrderBy(orderBy: Seq[SortOrder]): QueryAccumulator = copy(orderBy = Some(orderBy))

    /** @inheritdoc */
    override def output: Seq[Attribute] = outputExpressions.map(toAttributeReference(_, Seq.empty))

    /** @inheritdoc */
    override lazy val qualifier: String = igniteQueryContext.uniqueTableAlias

    /** @inheritdoc */
    override def withLimit(limit: Expression): QueryAccumulator = copy(limit = Some(limit))

    /** @inheritdoc */
    override def withLocalLimit(localLimit: Expression): QueryAccumulator =  copy(localLimit = Some(localLimit))
}
