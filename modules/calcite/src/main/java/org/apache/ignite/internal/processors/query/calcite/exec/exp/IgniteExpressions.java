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

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMath;
import org.jetbrains.annotations.Nullable;

/** Calcite liq4j expressions customized for Ignite. */
public class IgniteExpressions {
    /** Make binary expression with arithmetic operations override. */
    public static Expression makeBinary(ExpressionType binaryType, Expression left, Expression right) {
        switch (binaryType) {
            case Add:
                return addExact(left, right);
            case Subtract:
                return subtractExact(left, right);
            case Multiply:
                return multiplyExact(left, right);
            case Divide:
                return divideExact(left, right);
            default:
                return Expressions.makeBinary(binaryType, left, right);
        }
    }

    /** Make unary expression with arithmetic operations override. */
    public static Expression makeUnary(ExpressionType unaryType, Expression operand) {
        switch (unaryType) {
            case Negate:
            case NegateChecked:
                return negateExact(unaryType, operand);
            default:
                return Expressions.makeUnary(unaryType, operand);
        }
    }

    /** Generate expression for method IgniteMath.addExact() for integer subtypes. */
    public static Expression addExact(Expression left, Expression right) {
        if (larger(left.getType(), right.getType()).isFixedNumeric())
            return Expressions.call(IgniteMath.class, "addExact", left, right);

        return Expressions.makeBinary(ExpressionType.Add, left, right);
    }

    /** Generate expression for method IgniteMath.subtractExact() for integer subtypes. */
    public static Expression subtractExact(Expression left, Expression right) {
        if (larger(left.getType(), right.getType()).isFixedNumeric())
            return Expressions.call(IgniteMath.class, "subtractExact", left, right);

        return Expressions.makeBinary(ExpressionType.Subtract, left, right);
    }

    /** Generate expression for method IgniteMath.multiplyExact() for integer subtypes. */
    public static Expression multiplyExact(Expression left, Expression right) {
        if (larger(left.getType(), right.getType()).isFixedNumeric())
            return Expressions.call(IgniteMath.class, "multiplyExact", left, right);

        return Expressions.makeBinary(ExpressionType.Multiply, left, right);
    }

    /** Generate expression for method IgniteMath.divideExact() for integer subtypes. */
    public static Expression divideExact(Expression left, Expression right) {
        if (larger(left.getType(), right.getType()).isFixedNumeric())
            return Expressions.call(IgniteMath.class, "divideExact", left, right);

        return Expressions.makeBinary(ExpressionType.Divide, left, right);
    }

    /** Generate expression for method IgniteMath.negateExact() for integer subtypes. */
    private static Expression negateExact(ExpressionType unaryType, Expression operand) {
        assert unaryType == ExpressionType.Negate || unaryType == ExpressionType.NegateChecked;

        Type opType = operand.getType();

        if (opType == Integer.TYPE || opType == Long.TYPE || opType == Short.TYPE || opType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "negateExact", operand);

        return Expressions.makeUnary(unaryType, operand);
    }

    /** Generate expression for conversion from numeric primitive to numeric primitive with bounds check. */
    public static Expression convertChecked(Expression exp, Primitive fromPrimitive, Primitive toPrimitive) {
        if (fromPrimitive.ordinal() <= toPrimitive.ordinal() || !toPrimitive.isFixedNumeric())
            return Expressions.convert_(exp, toPrimitive.primitiveClass);

        return Expressions.call(IgniteMath.class, "convertTo" +
            SqlFunctions.initcap(toPrimitive.primitiveName) + "Exact", exp);
    }

    /** Generate expression for conversion from string to numeric primitive with bounds check. */
    public static Expression parseStringChecked(Expression exp, Primitive toPrimitive) {
        return Expressions.call(IgniteMath.class, "convertTo" +
            SqlFunctions.initcap(toPrimitive.primitiveName) + "Exact", Expressions.new_(BigDecimal.class, exp));
    }

    /** Generate expression for conversion from Number to numeric primitive with bounds check. */
    public static Expression unboxChecked(Expression exp, @Nullable Primitive fromBox, Primitive toPrimitive) {
        if ((fromBox != null && fromBox.ordinal() <= toPrimitive.ordinal()) || !toPrimitive.isFixedNumeric())
            return Expressions.unbox(exp, toPrimitive);

        return Expressions.call(IgniteMath.class, "convertTo" +
            SqlFunctions.initcap(toPrimitive.primitiveName) + "Exact", exp);
    }

    /** Find larger in type hierarchy. */
    private static Primitive larger(Type type0, Type type1) {
        Primitive primitive0 = Primitive.ofBoxOr(type0);
        Primitive primitive1 = Primitive.ofBoxOr(type1);

        return primitive0.ordinal() > primitive1.ordinal() ? primitive0 : primitive1;
    }
}
