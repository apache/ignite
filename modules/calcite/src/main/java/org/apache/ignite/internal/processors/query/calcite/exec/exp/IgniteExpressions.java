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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMath;

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

    /** */
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
        Type largerType = larger(left.getType(), right.getType());

        if (largerType == Integer.TYPE || largerType == Long.TYPE || largerType == Short.TYPE || largerType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "addExact", left, right);

        return Expressions.makeBinary(ExpressionType.Add, left, right);
    }

    /** Generate expression for method IgniteMath.subtractExact() for integer subtypes. */
    public static Expression subtractExact(Expression left, Expression right) {
        Type largerType = larger(left.getType(), right.getType());

        if (largerType == Integer.TYPE || largerType == Long.TYPE || largerType == Short.TYPE || largerType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "subtractExact", left, right);

        return Expressions.makeBinary(ExpressionType.Subtract, left, right);
    }

    /** Generate expression for method IgniteMath.multiplyExact() for integer subtypes. */
    public static Expression multiplyExact(Expression left, Expression right) {
        Type largerType = larger(left.getType(), right.getType());

        if (largerType == Integer.TYPE || largerType == Long.TYPE || largerType == Short.TYPE || largerType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "multiplyExact", left, right);

        return Expressions.makeBinary(ExpressionType.Multiply, left, right);
    }

    /** Generate expression for method IgniteMath.divideExact() for integer subtypes. */
    public static Expression divideExact(Expression left, Expression right) {
        Type largerType = larger(left.getType(), right.getType());

        if (largerType == Integer.TYPE || largerType == Long.TYPE || largerType == Short.TYPE || largerType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "divideExact", left, right);

        return Expressions.makeBinary(ExpressionType.Divide, left, right);
    }

    /** Generate expression for method IgniteMath.convertToIntExact(). */
    public static Expression convertToIntExact(Expression exp) {
        Type type = exp.getType();

        if (type == Long.TYPE || type == Long.class)
            return Expressions.call(IgniteMath.class, "convertToIntExact", exp);

        return exp;
    }

    /** Generate expression for method IgniteMath.convertToShortExact(). */
    public static Expression convertToShortExact(Expression exp) {
        Type type = exp.getType();

        if (type == Long.TYPE || type == Long.class || type == Integer.TYPE || type == Integer.class)
            return Expressions.call(IgniteMath.class, "convertToShortExact", exp);

        return exp;
    }

    /** Generate expression for method IgniteMath.convertToByteExact(). */
    public static Expression convertToByteExact(Expression exp) {
        Type type = exp.getType();

        if (type == Long.TYPE || type == Long.class || type == Integer.TYPE || type == Integer.class
            || type == Short.TYPE || type == Short.class)
            return Expressions.call(IgniteMath.class, "convertToByteExact", exp);

        return exp;
    }

    /** Generate expression for method IgniteMath.negateExact() for integer subtypes. */
    private static Expression negateExact(ExpressionType unaryType, Expression operand) {
        assert unaryType == ExpressionType.Negate || unaryType == ExpressionType.NegateChecked;

        Type opType = operand.getType();

        if (opType == Integer.TYPE || opType == Long.TYPE || opType == Short.TYPE || opType == Byte.TYPE)
            return Expressions.call(IgniteMath.class, "negateExact", operand);

        return Expressions.makeUnary(unaryType, operand);
    }

    /** Find larger in type hierarchy. */
    private static Type larger(Type type0, Type type1) {
        if (type0 != Double.TYPE && type0 != Double.class && type1 != Double.TYPE && type1 != Double.class)
            if (type0 != Float.TYPE && type0 != Float.class && type1 != Float.TYPE && type1 != Float.class)
                if (type0 != Long.TYPE && type0 != Long.class && type1 != Long.TYPE && type1 != Long.class)
                    if (type0 != Integer.TYPE && type0 != Integer.class && type1 != Integer.TYPE && type1 != Integer.class)
                        return type0 != Short.TYPE && type0 != Short.class && type1 != Short.TYPE && type1 != Short.class
                            ? Byte.TYPE : Short.TYPE;
                    else
                        return Integer.TYPE;
                else
                    return Long.TYPE;
            else
                return Float.TYPE;
        else
            return Double.TYPE;
    }
}
