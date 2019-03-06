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

package org.apache.ignite.spark.impl.optimization

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/**
  * Object to support aggregate expressions like `sum` or `avg`.
  */
private[optimization] object AggregateExpressions extends SupportedExpressions {
    /** @inheritdoc */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean = expr match {
        case AggregateExpression(aggregateFunction, _, _, _) ⇒
            checkChild(aggregateFunction)

        case Average(child) ⇒
            checkChild(child)

        case Count(children) ⇒
            children.forall(checkChild)

        case Max(child) ⇒
            checkChild(child)

        case Min(child) ⇒
            checkChild(child)

        case Sum(child) ⇒
            checkChild(child)

        case _ ⇒
            false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case AggregateExpression(aggregateFunction, _, isDistinct, _) ⇒
            aggregateFunction match {
                case Count(children) ⇒
                    if (isDistinct)
                        Some(s"COUNT(DISTINCT ${children.map(childToString(_)).mkString(" ")})")
                    else
                        Some(s"COUNT(${children.map(childToString(_)).mkString(" ")})")

                case sum: Sum ⇒
                    if (isDistinct)
                        Some(castSum(
                            s"SUM(DISTINCT ${sum.children.map(childToString(_)).mkString(" ")})", sum.dataType))
                    else
                        Some(castSum(s"SUM(${sum.children.map(childToString(_)).mkString(" ")})", sum.dataType))

                case _ ⇒
                    Some(childToString(aggregateFunction))
            }

        case Average(child) ⇒
            child.dataType match {
                case DecimalType() | DoubleType ⇒
                    Some(s"AVG(${childToString(child)})")

                case _ ⇒
                    //Spark `AVG` return type is always a double or a decimal.
                    //See [[org.apache.spark.sql.catalyst.expressions.aggregate.Average]]
                    //But Ignite `AVG` return type for a integral types is integral.
                    //To preserve query correct results has to cast column to double.
                    Some(s"AVG(CAST(${childToString(child)} AS DOUBLE))")
            }


        case Count(children) ⇒
            Some(s"COUNT(${children.map(childToString(_)).mkString(" ")})")

        case Max(child) ⇒
            Some(s"MAX(${childToString(child)})")

        case Min(child) ⇒
            Some(s"MIN(${childToString(child)})")

        case sum: Sum ⇒
            Some(castSum(s"SUM(${childToString(sum.child)})", sum.dataType))

        case _ ⇒
            None
    }

    /**
      * Ignite returns BigDecimal but Spark expects BIGINT.
      */
    private def castSum(sumSql: String, dataType: DataType): String = dataType match {
        case LongType ⇒
            s"CAST($sumSql AS BIGINT)"

        case _ ⇒
            s"$sumSql"
    }
}
