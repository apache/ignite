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

import org.apache.spark.sql.catalyst.expressions.{Expression, _}

/**
  * Object to support condition expression. Like `and` or `in` operators.
  */
private[optimization] object ConditionExpressions extends SupportedExpressions {
    /** @inheritdoc */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean = expr match {
        case EqualTo(left, right) ⇒
            checkChild(left) && checkChild(right)

        case EqualNullSafe(left, right) ⇒
            checkChild(left) && checkChild(right)

        case GreaterThan(left, right) ⇒
            checkChild(left) && checkChild(right)

        case GreaterThanOrEqual(left, right) ⇒
            checkChild(left) && checkChild(right)

        case LessThan(left, right) ⇒
            checkChild(left) && checkChild(right)

        case LessThanOrEqual(left, right) ⇒
            checkChild(left) && checkChild(right)

        case InSet(child, set) if set.forall(_.isInstanceOf[Literal]) ⇒
            checkChild(child)

        case In(child, list) if list.forall(_.isInstanceOf[Literal]) ⇒
            checkChild(child)

        case IsNull(child) ⇒
            checkChild(child)

        case IsNotNull(child) ⇒
            checkChild(child)

        case And(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Or(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Not(child) ⇒
            checkChild(child)

        case StartsWith(left, right) ⇒
            checkChild(left) && checkChild(right)

        case EndsWith(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Contains(left, right) ⇒
            checkChild(left) && checkChild(right)

        case _ ⇒
            false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case EqualTo(left, right) ⇒
            Some(s"${childToString(left)} = ${childToString(right)}")

        case EqualNullSafe(left, right) ⇒
            Some(s"(${childToString(left)} IS NULL OR ${childToString(left)} = ${childToString(right)})")

        case GreaterThan(left, right) ⇒
            Some(s"${childToString(left)} > ${childToString(right)}")

        case GreaterThanOrEqual(left, right) ⇒
            Some(s"${childToString(left)} >= ${childToString(right)}")

        case LessThan(left, right) ⇒
            Some(s"${childToString(left)} < ${childToString(right)}")

        case LessThanOrEqual(left, right) ⇒
            Some(s"${childToString(left)} <= ${childToString(right)}")

        case In(attr, values) ⇒
            Some(s"${childToString(attr)} IN (${values.map(childToString(_)).mkString(", ")})")

        case IsNull(child) ⇒
            Some(s"${childToString(child)} IS NULL")

        case IsNotNull(child) ⇒
            Some(s"${childToString(child)} IS NOT NULL")

        case And(left, right) ⇒
            Some(s"${childToString(left)} AND ${childToString(right)}")

        case Or(left, right) ⇒
            Some(s"${childToString(left)} OR ${childToString(right)}")

        case Not(child) ⇒
            Some(s"NOT ${childToString(child)}")

        case StartsWith(attr, value) ⇒ {
            //Expecting string literal here.
            //To add % sign it's required to remove quotes.
            val valStr = removeQuotes(childToString(value))

            Some(s"${childToString(attr)} LIKE '$valStr%'")
        }

        case EndsWith(attr, value) ⇒ {
            //Expecting string literal here.
            //To add % sign it's required to remove quotes.
            val valStr = removeQuotes(childToString(value))

            Some(s"${childToString(attr)} LIKE '%$valStr'")
        }

        case Contains(attr, value) ⇒ {
            //Expecting string literal here.
            //To add % signs it's required to remove quotes.
            val valStr = removeQuotes(childToString(value))

            Some(s"${childToString(attr)} LIKE '%$valStr%'")
        }

        case _ ⇒
            None
    }

    /**
      * @param str String to process.
      * @return Str without surrounding quotes.
      */
    private def removeQuotes(str: String): String =
        if (str.length < 2)
            str
        else
            str match {
                case quoted if quoted.startsWith("'") && quoted.endsWith("'") ⇒
                    quoted.substring(1, quoted.length-1)

                case _ ⇒ str
            }
}
