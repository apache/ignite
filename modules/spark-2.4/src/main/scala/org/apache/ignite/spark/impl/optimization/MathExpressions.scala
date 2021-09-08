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

package org.apache.ignite.spark.impl.optimization

import org.apache.spark.sql.catalyst.expressions.{Expression, _}

/**
  * Object to support math expressions.
  */
private[optimization] object MathExpressions extends SupportedExpressions {
    /** @inheritdoc */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean = expr match {
        case Abs(child) ⇒
            checkChild(child)

        case Acos(child) ⇒
            checkChild(child)

        case Asin(child) ⇒
            checkChild(child)

        case Atan(child) ⇒
            checkChild(child)

        case Cos(child) ⇒
            checkChild(child)

        case Cosh(child) ⇒
            checkChild(child)

        case Sin(child) ⇒
            checkChild(child)

        case Sinh(child) ⇒
            checkChild(child)

        case Tan(child) ⇒
            checkChild(child)

        case Tanh(child) ⇒
            checkChild(child)

        case Atan2(left, right) ⇒
            checkChild(left) && checkChild(right)

        case BitwiseAnd(left, right) ⇒
            checkChild(left) && checkChild(right)

        case BitwiseOr(left, right) ⇒
            checkChild(left) && checkChild(right)

        case BitwiseXor(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Ceil(child) ⇒
            checkChild(child)

        case ToDegrees(child) ⇒
            checkChild(child)

        case Exp(child) ⇒
            checkChild(child)

        case Floor(child) ⇒
            checkChild(child)

        case Log(child) ⇒
            checkChild(child)

        case Log10(child) ⇒
            checkChild(child)

        case Logarithm(left, right) ⇒
            checkChild(left) && checkChild(right)

        case ToRadians(child) ⇒
            checkChild(child)

        case Sqrt(child) ⇒
            checkChild(child)

        case _: Pi ⇒
            true

        case _: EulerNumber ⇒
            true

        case Pow(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Rand(child) ⇒
            checkChild(child)

        case Round(child, scale) ⇒
            checkChild(child) && checkChild(scale)

        case Signum(child) ⇒
            checkChild(child)

        case Remainder(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Divide(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Multiply(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Subtract(left, right) ⇒
            checkChild(left) && checkChild(right)

        case Add(left, right) ⇒
            checkChild(left) && checkChild(right)

        case UnaryMinus(child) ⇒
            checkChild(child)

        case UnaryPositive(child) ⇒
            checkChild(child)

        case _ ⇒ false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case Abs(child) ⇒
            Some(s"ABS(${childToString(child)})")

        case Acos(child) ⇒
            Some(s"ACOS(${childToString(child)})")

        case Asin(child) ⇒
            Some(s"ASIN(${childToString(child)})")

        case Atan(child) ⇒
            Some(s"ATAN(${childToString(child)})")

        case Cos(child) ⇒
            Some(s"COS(${childToString(child)})")

        case Cosh(child) ⇒
            Some(s"COSH(${childToString(child)})")

        case Sin(child) ⇒
            Some(s"SIN(${childToString(child)})")

        case Sinh(child) ⇒
            Some(s"SINH(${childToString(child)})")

        case Tan(child) ⇒
            Some(s"TAN(${childToString(child)})")

        case Tanh(child) ⇒
            Some(s"TANH(${childToString(child)})")

        case Atan2(left, right) ⇒
            Some(s"ATAN2(${childToString(left)}, ${childToString(right)})")

        case BitwiseAnd(left, right) ⇒
            Some(s"BITAND(${childToString(left)}, ${childToString(right)})")

        case BitwiseOr(left, right) ⇒
            Some(s"BITOR(${childToString(left)}, ${childToString(right)})")

        case BitwiseXor(left, right) ⇒
            Some(s"BITXOR(${childToString(left)}, ${childToString(right)})")

        case Ceil(child) ⇒
            Some(s"CAST(CEIL(${childToString(child)}) AS LONG)")

        case ToDegrees(child) ⇒
            Some(s"DEGREES(${childToString(child)})")

        case Exp(child) ⇒
            Some(s"EXP(${childToString(child)})")

        case Floor(child) ⇒
            Some(s"CAST(FLOOR(${childToString(child)}) AS LONG)")

        case Log(child) ⇒
            Some(s"LOG(${childToString(child)})")

        case Log10(child) ⇒
            Some(s"LOG10(${childToString(child)})")

        case Logarithm(base, arg) ⇒
            childToString(base) match {
                //Spark internally converts LN(XXX) to LOG(2.718281828459045, XXX).
                //Because H2 doesn't have builtin function for a free base logarithm
                //I want to prevent usage of log(a, b) = ln(a)/ln(b) when possible.
                case "2.718281828459045" ⇒
                    Some(s"LOG(${childToString(arg)})")
                case "10" ⇒
                    Some(s"LOG10(${childToString(arg)})")
                case argStr ⇒
                    Some(s"(LOG(${childToString(arg)})/LOG($argStr))")
            }

        case ToRadians(child) ⇒
            Some(s"RADIANS(${childToString(child)})")

        case Sqrt(child) ⇒
            Some(s"SQRT(${childToString(child)})")

        case _: Pi ⇒
            Some("PI()")

        case _: EulerNumber ⇒
            Some("E()")

        case Pow(left, right) ⇒
            Some(s"POWER(${childToString(left)}, ${childToString(right)})")

        case Rand(child) ⇒
            Some(s"RAND(${childToString(child)})")

        case Round(child, scale) ⇒
            Some(s"ROUND(${childToString(child)}, ${childToString(scale)})")

        case Signum(child) ⇒
            Some(s"SIGN(${childToString(child)})")

        case Remainder(left, right) ⇒
            Some(s"${childToString(left)} % ${childToString(right)}")

        case Divide(left, right) ⇒
            Some(s"${childToString(left)} / ${childToString(right)}")

        case Multiply(left, right) ⇒
            Some(s"${childToString(left)} * ${childToString(right)}")

        case Subtract(left, right) ⇒
            Some(s"${childToString(left)} - ${childToString(right)}")

        case Add(left, right) ⇒
            Some(s"${childToString(left)} + ${childToString(right)}")

        case UnaryMinus(child) ⇒
            Some(s"-${childToString(child)}")

        case UnaryPositive(child) ⇒
            Some(s"+${childToString(child)}")

        case _ ⇒
            None
    }
}
