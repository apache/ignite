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

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
  * Object to support some 'simple' expressions like aliases.
  */
private[optimization] object SimpleExpressions extends SupportedExpressions {
    /** @inheritdoc */
    override def apply(expr: Expression, checkChild: Expression ⇒ Boolean): Boolean = expr match {
        case Literal(_, _) ⇒
            true

        case _: Attribute ⇒
            true

        case Alias(child, _) ⇒
            checkChild(child)

        case Cast(child, dataType, _) ⇒
            checkChild(child) && castSupported(from = child.dataType, to = dataType)

        case _ ⇒
            false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case l: Literal ⇒
            if (l.value == null)
                Some("null")
            else {
                l.dataType match {
                    case StringType ⇒
                        Some("'" + l.value.toString + "'")

                    case TimestampType ⇒
                        l.value match {
                            //Internal representation of TimestampType is Long.
                            //So we converting from internal spark representation to CAST call.
                            case date: Long ⇒
                                Some(s"CAST('${timestampFormat.get.format(DateTimeUtils.toJavaTimestamp(date))}' " +
                                    s"AS TIMESTAMP)")

                            case _ ⇒
                                Some(l.value.toString)
                        }

                    case DateType ⇒
                        l.value match {
                            //Internal representation of DateType is Int.
                            //So we converting from internal spark representation to CAST call.
                            case days: Integer ⇒
                                val date = new java.util.Date(DateTimeUtils.daysToMillis(days))

                                Some(s"CAST('${dateFormat.get.format(date)}' AS DATE)")

                            case _ ⇒
                                Some(l.value.toString)
                        }

                    case _ ⇒
                        Some(l.value.toString)
                }
            }
        case ar: AttributeReference ⇒
            val name =
                if (useQualifier)
                    ar.qualifier.map(_ + "." + ar.name).getOrElse(ar.name)
                else
                    ar.name

            if (ar.metadata.contains(ALIAS) &&
                !isAliasEqualColumnName(ar.metadata.getString(ALIAS), ar.name) &&
                useAlias) {
                Some(aliasToString(name, ar.metadata.getString(ALIAS)))
            } else
                Some(name)

        case Alias(child, name) ⇒
            if (useAlias)
                Some(childToString(child)).map(aliasToString(_, name))
            else
                Some(childToString(child))

        case Cast(child, dataType, _) ⇒
            Some(s"CAST(${childToString(child)} AS ${toSqlType(dataType)})")

        case SortOrder(child, direction, _, _) ⇒
            Some(s"${childToString(child)}${if(direction==Descending) " DESC" else ""}")

        case _ ⇒
            None
    }

    /**
      * @param column Column name.
      * @param alias Alias.
      * @return SQL String for column with alias.
      */
    private def aliasToString(column: String, alias: String): String =
        if (isAliasEqualColumnName(alias, column))
            column
        else if (alias.matches("[A-Za-z_][0-9A-Za-z_]*"))
            s"$column AS $alias"
        else
            s"""$column AS "$alias""""

    /**
      * @param alias Alias.
      * @param column Column.
      * @return True if name equals to alias, false otherwise.
      */
    private def isAliasEqualColumnName(alias: String, column: String): Boolean =
        alias.compareToIgnoreCase(column.replaceAll("'", "")) == 0

    /**
      * @param from From type conversion.
      * @param to To type conversion.
      * @return True if cast support for types, false otherwise.
      */
    private def castSupported(from: DataType, to: DataType): Boolean = from match {
        case BooleanType ⇒
            Set[DataType](BooleanType, StringType)(to)

        case ByteType ⇒
            Set(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, DecimalType(_, _),
                StringType)(to)

        case ShortType ⇒
            Set(ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, DecimalType(_, _))(to)

        case IntegerType ⇒
            Set(IntegerType, LongType, FloatType, DoubleType, StringType, DecimalType(_, _))(to)

        case LongType ⇒
            Set(LongType, FloatType, DoubleType, StringType, DecimalType(_, _))(to)

        case FloatType ⇒
            Set(FloatType, DoubleType, StringType, DecimalType(_, _))(to)

        case DoubleType ⇒
            Set(DoubleType, StringType, DecimalType(_, _))(to)

        case DecimalType() ⇒
            Set(StringType, DecimalType(_, _))(to)

        case DateType ⇒
            Set[DataType](DateType, StringType, LongType, TimestampType)(to)

        case TimestampType ⇒
            Set[DataType](TimestampType, DateType, StringType, LongType)(to)

        case StringType ⇒
            Set(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
                DecimalType(_, _), DateType, TimestampType, StringType)(to)

        case BinaryType ⇒
            false

        case ArrayType(_, _) ⇒
            false
    }

    /**
      * Date format built-in Ignite.
      */
    private val dateFormat: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat] {
        override def initialValue(): SimpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd")
    }

    /**
      * Timestamp format built-in Ignite.
      */
    private val timestampFormat: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat] {
        override def initialValue(): SimpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    }
}
