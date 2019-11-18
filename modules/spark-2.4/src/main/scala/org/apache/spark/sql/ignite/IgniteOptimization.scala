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

package org.apache.spark.sql.ignite

import org.apache.ignite.IgniteException
import org.apache.ignite.spark.impl.{IgniteSQLAccumulatorRelation, IgniteSQLRelation, sqlCacheName}
import org.apache.ignite.spark.impl.optimization.{accumulator, _}
import org.apache.ignite.spark.impl.optimization.accumulator._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * Query plan optimization for a Ignite based queries.
  */
object IgniteOptimization extends Rule[LogicalPlan] with Logging {
    /** @inheritdoc */
    override def apply(plan: LogicalPlan): LogicalPlan = {
        logDebug("")
        logDebug("== Plan Before Ignite Operator Push Down ==")
        logDebug(plan.toString())

        val transformed = fixAmbiguousOutput(pushDownOperators(plan))

        logDebug("")
        logDebug("== Plan After Ignite Operator Push Down ==")
        logDebug(transformed.toString())

        makeIgniteAccRelation(transformed)
    }

    /**
      * Change query plan by accumulating query parts supported by Ignite into `QueryAccumulator`.
      *
      * @param plan Query plan.
      * @return Transformed plan.
      */
    private def pushDownOperators(plan: LogicalPlan): LogicalPlan = {
        val aliasIndexIterator = Stream.from(1).iterator

        //Flag to indicate that some step was skipped due to unsupported expression.
        //When it true we has to skip entire transformation of higher level Nodes.
        var stepSkipped = true

        //Applying optimization rules from bottom to up tree nodes.
        plan.transformUp {
            //We found basic node to transform.
            //We create new accumulator and going to the upper layers.
            case LogicalRelation(igniteSqlRelation: IgniteSQLRelation[_, _], output, _catalogTable, _) ⇒
                //Clear flag to optimize each statement separately
                stepSkipped = false

                val igniteQueryContext = IgniteQueryContext(
                    igniteContext = igniteSqlRelation.ic,
                    sqlContext = igniteSqlRelation.sqlContext,
                    catalogTable = _catalogTable,
                    aliasIndex = aliasIndexIterator,
                    cacheName =
                        sqlCacheName(igniteSqlRelation.ic.ignite(), igniteSqlRelation.tableName,
                            igniteSqlRelation.schemaName)
                            .getOrElse(throw new IgniteException("Unknown table")))

                //Logical Relation is bottomest TreeNode in LogicalPlan.
                //We replace it with accumulator.
                //We push all supported SQL operator into it on the higher tree levels.
                SingleTableSQLAccumulator(
                    igniteQueryContext = igniteQueryContext,
                    table = Some(igniteSqlRelation.tableName),
                    tableExpression = None,
                    outputExpressions = output.map(attr ⇒ attr.withQualifier(Some(igniteSqlRelation.tableName))))

            case project: Project if !stepSkipped && exprsAllowed(project.projectList) ⇒
                //Project layer just changes output of current query.
                project.child match {
                    case acc: SelectAccumulator ⇒
                        acc.withOutputExpressions(
                            substituteExpressions(project.projectList, acc.outputExpressions))

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case sort: Sort if !stepSkipped && isSortPushDownAllowed(sort.order, sort.global) ⇒
                sort.child match {
                    case acc: QueryAccumulator ⇒
                        acc.withOrderBy(sort.order)

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case filter: Filter if !stepSkipped && exprsAllowed(filter.condition) ⇒

                filter.child match {
                    case acc: SelectAccumulator ⇒
                        if (hasAggregateInside(filter.condition) || acc.groupBy.isDefined)
                            acc.withHaving(acc.having.getOrElse(Nil) :+ filter.condition)
                        else
                            acc.withWhere(acc.where.getOrElse(Nil) :+ filter.condition)

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case agg: Aggregate
                if !stepSkipped && exprsAllowed(agg.groupingExpressions) && exprsAllowed(agg.aggregateExpressions) ⇒

                agg.child match {
                    case acc: SelectAccumulator ⇒
                        if (acc.groupBy.isDefined) {
                            val tableAlias = acc.igniteQueryContext.uniqueTableAlias

                            SingleTableSQLAccumulator(
                                igniteQueryContext = acc.igniteQueryContext,
                                table = None,
                                tableExpression = Some((acc, tableAlias)),
                                outputExpressions = agg.aggregateExpressions)
                        }
                        else
                            acc
                                .withGroupBy(agg.groupingExpressions)
                                .withOutputExpressions(
                                    substituteExpressions(agg.aggregateExpressions, acc.outputExpressions))

                    case acc: QueryAccumulator ⇒
                        val tableAlias = acc.igniteQueryContext.uniqueTableAlias

                        SingleTableSQLAccumulator(
                            igniteQueryContext = acc.igniteQueryContext,
                            table = None,
                            tableExpression = Some((acc, tableAlias)),
                            outputExpressions = agg.aggregateExpressions)

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case limit: LocalLimit if !stepSkipped && exprsAllowed(limit.limitExpr) ⇒
                limit.child match {
                    case acc: SelectAccumulator ⇒
                        acc.withLocalLimit(limit.limitExpr)

                    case acc: QueryAccumulator ⇒
                        acc.withLocalLimit(limit.limitExpr)

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case limit: GlobalLimit if !stepSkipped && exprsAllowed(limit.limitExpr) ⇒
                limit.child.transformUp {
                    case acc: SelectAccumulator ⇒
                        acc.withLimit(limit.limitExpr)

                    case acc: QueryAccumulator ⇒
                        acc.withLimit(limit.limitExpr)

                    case _ ⇒
                        throw new IgniteException("stepSkipped == true but child is not SelectAccumulator")
                }

            case union: Union if !stepSkipped && isAllChildrenOptimized(union.children) ⇒
                val first = union.children.head.asInstanceOf[QueryAccumulator]

                val subQueries = union.children.map(_.asInstanceOf[QueryAccumulator])

                UnionSQLAccumulator(
                    first.igniteQueryContext,
                    subQueries,
                    subQueries.head.output)

            case join: Join
                if !stepSkipped && isAllChildrenOptimized(Seq(join.left, join.right)) &&
                    join.condition.forall(exprsAllowed) ⇒

                val left = join.left.asInstanceOf[QueryAccumulator]

                val (leftOutput, leftAlias) =
                    if (!isSimpleTableAcc(left)) {
                        val tableAlias = left.igniteQueryContext.uniqueTableAlias

                        (left.output, Some(tableAlias))
                    }
                    else
                        (left.output, None)

                val right = join.right.asInstanceOf[QueryAccumulator]

                val (rightOutput, rightAlias) =
                    if (!isSimpleTableAcc(right) ||
                        leftAlias.getOrElse(left.qualifier) == right.qualifier) {
                        val tableAlias = right.igniteQueryContext.uniqueTableAlias

                        (right.output, Some(tableAlias))
                    }
                    else
                        (right.output, None)

                JoinSQLAccumulator(
                    left.igniteQueryContext,
                    left,
                    right,
                    join.joinType,
                    leftOutput ++ rightOutput,
                    join.condition,
                    leftAlias,
                    rightAlias)

            case unknown ⇒
                stepSkipped = true

                unknown
        }
    }

    /**
      * Changes qualifiers for an ambiguous columns names.
      *
      * @param plan Query plan.
      * @return Transformed plan.
      */
    private def fixAmbiguousOutput(plan: LogicalPlan): LogicalPlan = plan.transformDown {
        case acc: SingleTableSQLAccumulator if acc.children.exists(_.isInstanceOf[JoinSQLAccumulator]) ⇒
            val fixedChildOutput =
                fixAmbiguousOutput(acc.tableExpression.get._1.outputExpressions, acc.igniteQueryContext)

            val newOutput = substituteExpressions(acc.outputExpressions, fixedChildOutput, changeOnlyName = true)

            acc.copy(
                outputExpressions = newOutput,
                where = acc.where.map(
                    substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                groupBy = acc.groupBy.map(
                    substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                having = acc.having.map(
                    substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                limit = acc.limit.map(
                    substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                localLimit = acc.localLimit.map(
                    substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                orderBy = acc.orderBy.map(
                    substituteExpressions(_, fixedChildOutput, changeOnlyName = true)))

            acc

        case acc: JoinSQLAccumulator
            if acc.left.isInstanceOf[JoinSQLAccumulator] || acc.right.isInstanceOf[JoinSQLAccumulator] ⇒
            val leftFixed = acc.left match {
                case leftJoin: JoinSQLAccumulator ⇒
                    val fixedChildOutput = fixAmbiguousOutput(acc.left.outputExpressions, acc.igniteQueryContext)

                    val newOutput =
                        substituteExpressions(acc.outputExpressions, fixedChildOutput, changeOnlyName = true)

                    acc.copy(
                        outputExpressions = newOutput,
                        left = leftJoin.copy(outputExpressions = fixedChildOutput),
                        condition = acc.condition.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        where = acc.where.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        groupBy = acc.groupBy.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        having = acc.having.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        limit = acc.limit.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        localLimit = acc.localLimit.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        orderBy = acc.orderBy.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)))

                case _ ⇒ acc
            }

            val fixed = leftFixed.right match {
                case rightJoin: JoinSQLAccumulator ⇒
                    val fixedChildOutput =
                        fixAmbiguousOutput(leftFixed.outputExpressions, leftFixed.igniteQueryContext)

                    val newOutput = substituteExpressions(leftFixed.outputExpressions, fixedChildOutput)

                    leftFixed.copy(
                        outputExpressions = newOutput,
                        right = rightJoin.copy(outputExpressions = fixedChildOutput),
                        condition = acc.condition.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        where = acc.where.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        groupBy = acc.groupBy.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        having = acc.having.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)),
                        limit = acc.limit.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        localLimit = acc.localLimit.map(
                            substituteExpression(_, fixedChildOutput, changeOnlyName = true)),
                        orderBy = acc.orderBy.map(
                            substituteExpressions(_, fixedChildOutput, changeOnlyName = true)))

                case _ ⇒ leftFixed
            }

            fixed.copy(
                condition = acc.condition.map(
                    substituteExpression(_, acc.outputExpressions, changeOnlyName = true)),
                where = acc.where.map(
                    substituteExpressions(_, acc.outputExpressions, changeOnlyName = true)),
                groupBy = acc.groupBy.map(
                    substituteExpressions(_, acc.outputExpressions, changeOnlyName = true)),
                having = acc.having.map(
                    substituteExpressions(_, acc.outputExpressions, changeOnlyName = true)),
                limit = acc.limit.map(
                    substituteExpression(_, acc.outputExpressions, changeOnlyName = true)),
                localLimit = acc.localLimit.map(
                    substituteExpression(_, acc.outputExpressions, changeOnlyName = true)),
                orderBy = acc.orderBy.map(
                    substituteExpressions(_, acc.outputExpressions, changeOnlyName = true)))

        case unknown ⇒
            unknown
    }

    private def fixAmbiguousOutput(exprs: Seq[NamedExpression], ctx: IgniteQueryContext): Seq[NamedExpression] =
        exprs.foldLeft((Set[String](), Set[NamedExpression]())) {
            case ((uniqueNames, fixed), next) ⇒
                if (uniqueNames(next.name))
                    (uniqueNames, fixed + Alias(next, ctx.uniqueColumnAlias(next))(exprId = next.exprId))
                else
                    (uniqueNames + next.name, fixed + next)
        }._2.toSeq

    /**
      * Substitutes each `QueryAccumulator` with a `LogicalRelation` contains `IgniteSQLAccumulatorRelation`.
      *
      * @param plan Query plan.
      * @return Transformed plan.
      */
    private def makeIgniteAccRelation(plan: LogicalPlan): LogicalPlan =
        plan.transformDown {
            case acc: QueryAccumulator ⇒
                new LogicalRelation (
                    relation = IgniteSQLAccumulatorRelation(acc),
                    output = acc.outputExpressions.map(toAttributeReference(_, Seq.empty)),
                    catalogTable = acc.igniteQueryContext.catalogTable,
                    false)
        }

    /**
      * @param order Order.
      * @param global True if order applied to entire result set false if ordering per-partition.
      * @return True if sort can be pushed down to Ignite, false otherwise.
      */
    private def isSortPushDownAllowed(order: Seq[SortOrder], global: Boolean): Boolean =
        global && order.map(_.child).forall(exprsAllowed)

    /**
      * @param children Plans to check.
      * @return True is all plan are `QueryAccumulator`, false otherwise.
      */
    private def isAllChildrenOptimized(children: Seq[LogicalPlan]): Boolean =
        children.forall {
            case _: QueryAccumulator ⇒
                true

            case _ ⇒
                false
        }

    /**
      * Changes expression from `exprs` collection to expression with same `exprId` from `substitution`.
      *
      * @param exprs Expressions to substitute.
      * @param substitution Expressions for substitution
      * @param changeOnlyName If true substitute only expression name.
      * @tparam T Concrete expression type.
      * @return Substituted expressions.
      */
    private def substituteExpressions[T <: Expression](exprs: Seq[T], substitution: Seq[NamedExpression],
        changeOnlyName: Boolean = false): Seq[T] = {

        exprs.map(substituteExpression(_, substitution, changeOnlyName))
    }

    private def substituteExpression[T <: Expression](expr: T, substitution: Seq[NamedExpression],
        changeOnlyName: Boolean): T = expr match {
        case ne: NamedExpression ⇒
            substitution.find(_.exprId == ne.exprId) match {
                case Some(found) ⇒
                    if (!changeOnlyName)
                        found.asInstanceOf[T]
                    else ne match {
                        case alias: Alias ⇒
                            Alias(
                                AttributeReference(
                                    found.name,
                                    found.dataType,
                                    nullable = found.nullable,
                                    metadata = found.metadata)(
                                    exprId = found.exprId,
                                    qualifier = found.qualifier),
                                alias.name) (
                                exprId = alias.exprId,
                                qualifier = alias.qualifier,
                                explicitMetadata = alias.explicitMetadata).asInstanceOf[T]

                        case attr: AttributeReference ⇒
                            attr.copy(name = found.name)(
                                exprId = found.exprId,
                                qualifier = found.qualifier).asInstanceOf[T]

                        case _ ⇒ ne.asInstanceOf[T]
                    }

                case None ⇒
                    expr.withNewChildren(
                        substituteExpressions(expr.children, substitution, changeOnlyName)).asInstanceOf[T]
            }

        case _ ⇒
            expr.withNewChildren(
                substituteExpressions(expr.children, substitution, changeOnlyName)).asInstanceOf[T]
    }
}
