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

package org.apache.ignite.spark.impl


import org.apache.ignite.IgniteException
import org.apache.ignite.spark.impl.optimization.accumulator.{QueryAccumulator, SingleTableSQLAccumulator}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.types._

import scala.annotation.tailrec

/**
  */
package object optimization {
    /**
      * Constant to store alias in column metadata.
      */
    private[optimization] val ALIAS: String = "alias"

    /**
      * All `SupportedExpression` implementations.
      */
    private val SUPPORTED_EXPRESSIONS: List[SupportedExpressions] = List (
        SimpleExpressions,
        SystemExpressions,
        AggregateExpressions,
        ConditionExpressions,
        DateExpressions,
        MathExpressions,
        StringExpressions
    )

    /**
      * @param expr Expression.
      * @param useQualifier If true outputs attributes of `expr` with qualifier.
      * @param useAlias If true outputs `expr` with alias.
      * @return String representation of expression.
      */
    def exprToString(expr: Expression, useQualifier: Boolean = false, useAlias: Boolean = true): String = {
        @tailrec
        def exprToString0(expr: Expression, supportedExpressions: List[SupportedExpressions]): Option[String] =
            if (supportedExpressions.nonEmpty) {
                val exprStr = supportedExpressions.head.toString(
                    expr,
                    exprToString(_, useQualifier, useAlias = false),
                    useQualifier,
                    useAlias)

                exprStr match {
                    case res: Some[String] ⇒
                        res
                    case None ⇒
                        exprToString0(expr, supportedExpressions.tail)
                }
            }
            else
                None

        exprToString0(expr, SUPPORTED_EXPRESSIONS) match {
            case Some(str) ⇒ str

            case None ⇒
                throw new IgniteException("Unsupporte expression " + expr)
        }
    }

    /**
      * @param exprs Expressions to check.
      * @return True if `exprs` contains only allowed(i.e. can be pushed down to Ignite) expressions false otherwise.
      */
    def exprsAllowed(exprs: Seq[Expression]): Boolean =
        exprs.forall(exprsAllowed)

    /**
      * @param expr Expression to check.
      * @return True if `expr` allowed(i.e. can be pushed down to Ignite) false otherwise.
      *
      */
    def exprsAllowed(expr: Expression): Boolean =
        SUPPORTED_EXPRESSIONS.exists(_(expr, exprsAllowed))

    /**
      * Converts `input` into `AttributeReference`.
      *
      * @param input Expression to convert.
      * @param existingOutput Existing output.
      * @param exprId Optional expression ID to use.
      * @param alias Optional alias for a result.
      * @return Converted expression.
      */
    def toAttributeReference(input: Expression, existingOutput: Seq[NamedExpression], exprId: Option[ExprId] = None,
        alias: Option[String] = None): AttributeReference = {

        input match {
            case attr: AttributeReference ⇒
                val toCopy = existingOutput.find(_.exprId == attr.exprId).getOrElse(attr)

                AttributeReference(
                    name = toCopy.name,
                    dataType = toCopy.dataType,
                    metadata = alias
                        .map(new MetadataBuilder().withMetadata(toCopy.metadata).putString(ALIAS, _).build())
                        .getOrElse(toCopy.metadata)
                )(exprId = exprId.getOrElse(toCopy.exprId), qualifier = toCopy.qualifier)

            case a: Alias ⇒
                toAttributeReference(a.child, existingOutput, Some(a.exprId), Some(alias.getOrElse(a.name)))

            case agg: AggregateExpression ⇒
                agg.aggregateFunction match {
                    case c: Count ⇒
                        if (agg.isDistinct)
                            AttributeReference(
                                name = s"COUNT(DISTINCT ${c.children.map(exprToString(_)).mkString(" ")})",
                                dataType = LongType,
                                metadata = alias
                                    .map(new MetadataBuilder().putString(ALIAS, _).build())
                                    .getOrElse(Metadata.empty)
                            )(exprId = exprId.getOrElse(agg.resultId))
                        else
                            AttributeReference(
                                name = s"COUNT(${c.children.map(exprToString(_)).mkString(" ")})",
                                dataType = LongType,
                                metadata = alias
                                    .map(new MetadataBuilder().putString(ALIAS, _).build())
                                    .getOrElse(Metadata.empty)
                            )(exprId = exprId.getOrElse(agg.resultId))

                    case _ ⇒
                        toAttributeReference(agg.aggregateFunction, existingOutput, Some(exprId.getOrElse(agg.resultId)), alias)
                }

            case ne: NamedExpression ⇒
                AttributeReference(
                    name = exprToString(input),
                    dataType = input.dataType,
                    metadata = alias
                        .map(new MetadataBuilder().withMetadata(ne.metadata).putString(ALIAS, _).build())
                        .getOrElse(Metadata.empty)
                )(exprId = exprId.getOrElse(ne.exprId))

            case _ if exprsAllowed(input) ⇒
                AttributeReference(
                    name = exprToString(input),
                    dataType = input.dataType,
                    metadata = alias
                        .map(new MetadataBuilder().putString(ALIAS, _).build())
                        .getOrElse(Metadata.empty)
                )(exprId = exprId.getOrElse(NamedExpression.newExprId))

            case _  ⇒
                throw new IgniteException(s"Unsupported column expression $input")
        }
    }

    /**
      * @param dataType Spark data type.
      * @return SQL data type.
      */
    def toSqlType(dataType: DataType): String = dataType match {
        case BooleanType ⇒ "BOOLEAN"
        case IntegerType ⇒ "INT"
        case ByteType ⇒ "TINYINT"
        case ShortType ⇒ "SMALLINT"
        case LongType ⇒ "BIGINT"
        case DecimalType() ⇒ "DECIMAL"
        case DoubleType ⇒ "DOUBLE"
        case FloatType ⇒ "REAL"
        case DateType ⇒ "DATE"
        case TimestampType ⇒ "TIMESTAMP"
        case StringType ⇒ "VARCHAR"
        case BinaryType ⇒ "BINARY"
        case ArrayType(_, _) ⇒ "ARRAY"
        case _ ⇒
            throw new IgniteException(s"$dataType not supported!")
    }

    /**
      * @param expr Expression
      * @return True if expression or some of it children is AggregateExpression, false otherwise.
      */
    def hasAggregateInside(expr: Expression): Boolean = {
        def hasAggregateInside0(expr: Expression): Boolean = expr match {
            case AggregateExpression(_, _, _, _) ⇒
                true

            case e: Expression ⇒
                e.children.exists(hasAggregateInside0)
        }

        hasAggregateInside0(expr)
    }

    /**
      * Check if `acc` representing simple query.
      * Simple is `SELECT ... FROM table WHERE ... ` like query.
      * Without aggregation, limits, order, embedded select expressions.
      *
      * @param acc Accumulator to check.
      * @return True if accumulator stores simple query info, false otherwise.
      */
    def isSimpleTableAcc(acc: QueryAccumulator): Boolean = acc match {
        case acc: SingleTableSQLAccumulator if acc.table.isDefined ⇒
            acc.groupBy.isEmpty &&
                acc.localLimit.isEmpty &&
                acc.orderBy.isEmpty &&
                !acc.distinct &&
                !acc.outputExpressions.exists(hasAggregateInside)

        case _ ⇒
            false
    }
}
