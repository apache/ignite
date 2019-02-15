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

import org.apache.ignite.IgniteException
import org.apache.spark.sql.catalyst.expressions.{Coalesce, EqualTo, Expression, Greatest, If, IfNull, IsNotNull, IsNull, Least, Literal, NullIf, Nvl2}

/**
  * Object to support some built-in expressions like `nvl2` or `coalesce`.
  */
private[optimization] object SystemExpressions extends SupportedExpressions {
    /** @inheritdoc */
    override def apply(expr: Expression, checkChild: Expression ⇒ Boolean): Boolean = expr match {
        case Coalesce(children) ⇒
            children.forall(checkChild)

        case Greatest(children) ⇒
            children.forall(checkChild)

        case IfNull(left, right, _) ⇒
            checkChild(left) && checkChild(right)

        case Least(children) ⇒
            children.forall(checkChild)

        case NullIf(left, right, _) ⇒
            checkChild(left) && checkChild(right)

        case Nvl2(expr1, expr2, expr3, _) ⇒
            checkChild(expr1) && checkChild(expr2) && checkChild(expr3)

        case If(predicate, trueValue, falseValue) ⇒
            predicate match {
                case IsNotNull(child) ⇒
                    checkChild(child) && checkChild(trueValue) && checkChild(falseValue)

                case IsNull(child) ⇒
                    checkChild(child) && checkChild(trueValue) && checkChild(falseValue)

                case EqualTo(left, right) ⇒
                    trueValue match {
                        case Literal(null, _) ⇒
                            (left == falseValue || right == falseValue) && checkChild(left) && checkChild(right)

                        case _ ⇒
                            false
                    }

                case _ ⇒
                    false
            }

        case _ ⇒
            false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case Coalesce(children) ⇒
            Some(s"COALESCE(${children.map(childToString(_)).mkString(", ")})")

        case Greatest(children) ⇒
            Some(s"GREATEST(${children.map(childToString(_)).mkString(", ")})")

        case IfNull(left, right, _) ⇒
            Some(s"IFNULL(${childToString(left)}, ${childToString(right)})")

        case Least(children) ⇒
            Some(s"LEAST(${children.map(childToString(_)).mkString(", ")})")

        case NullIf(left, right, _) ⇒
            Some(s"NULLIF(${childToString(left)}, ${childToString(right)})")

        case Nvl2(expr1, expr2, expr3, _) ⇒
            Some(s"NVL2(${childToString(expr1)}, ${childToString(expr2)}, ${childToString(expr3)})")

        case If(predicate, trueValue, falseValue) ⇒
            predicate match {
                case IsNotNull(child) ⇒
                    Some(s"NVL2(${childToString(child)}, ${childToString(trueValue)}, ${childToString(falseValue)})")

                case IsNull(child) ⇒
                    Some(s"NVL2(${childToString(child)}, ${childToString(falseValue)}, ${childToString(trueValue)})")

                case EqualTo(left, right) ⇒
                    trueValue match {
                        case Literal(null, _) ⇒
                            if (left == falseValue)
                                Some(s"NULLIF(${childToString(left)}, ${childToString(right)})")
                            else if (right == falseValue)
                                Some(s"NULLIF(${childToString(right)}, ${childToString(left)})")
                            else
                                throw new IgniteException(s"Expression not supported. $expr")

                        case _ ⇒
                            throw new IgniteException(s"Expression not supported. $expr")
                    }

                case _ ⇒
                    throw new IgniteException(s"Expression not supported. $expr")
            }

        case _ ⇒
            None
    }
}
