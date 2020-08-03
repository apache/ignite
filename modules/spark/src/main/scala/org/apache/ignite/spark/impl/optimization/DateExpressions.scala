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
  * Object to support expressions to work with date/timestamp.
  */
private[optimization] object DateExpressions extends SupportedExpressions {
    /** @inheritdoc */
    def apply(expr: Expression, checkChild: (Expression) ⇒ Boolean): Boolean = expr match {
        case CurrentDate(None) ⇒
            true

        case CurrentTimestamp() ⇒
            true

        case DateAdd(startDate, days) ⇒
            checkChild(startDate) && checkChild(days)

        case DateDiff(date1, date2) ⇒
            checkChild(date1) && checkChild(date2)

        case DayOfMonth(date) ⇒
            checkChild(date)

        case DayOfYear(date) ⇒
            checkChild(date)

        case Hour(date, _) ⇒
            checkChild(date)

        case Minute(date, _) ⇒
            checkChild(date)

        case Month(date) ⇒
            checkChild(date)

        case ParseToDate(left, format, child) ⇒
            checkChild(left) && (format.isEmpty || checkChild(format.get)) && checkChild(child)

        case Quarter(date) ⇒
            checkChild(date)

        case Second(date, _) ⇒
            checkChild(date)

        case WeekOfYear(date) ⇒
            checkChild(date)

        case Year(date) ⇒
            checkChild(date)

        case _ ⇒
            false
    }

    /** @inheritdoc */
    override def toString(expr: Expression, childToString: Expression ⇒ String, useQualifier: Boolean,
        useAlias: Boolean): Option[String] = expr match {
        case CurrentDate(_) ⇒
            Some(s"CURRENT_DATE()")

        case CurrentTimestamp() ⇒
            Some(s"CURRENT_TIMESTAMP()")

        case DateAdd(startDate, days) ⇒
            Some(s"CAST(DATEADD('DAY', ${childToString(days)}, ${childToString(startDate)}) AS DATE)")

        case DateDiff(date1, date2) ⇒
            Some(s"CAST(DATEDIFF('DAY', ${childToString(date1)}, ${childToString(date2)}) AS INT)")

        case DayOfMonth(date) ⇒
            Some(s"DAY_OF_MONTH(${childToString(date)})")

        case DayOfYear(date) ⇒
            Some(s"DAY_OF_YEAR(${childToString(date)})")

        case Hour(date, _) ⇒
            Some(s"HOUR(${childToString(date)})")

        case Minute(date, _) ⇒
            Some(s"MINUTE(${childToString(date)})")

        case Month(date) ⇒
            Some(s"MINUTE(${childToString(date)})")

        case ParseToDate(left, formatOption, _) ⇒
            formatOption match {
                case Some(format) ⇒
                    Some(s"PARSEDATETIME(${childToString(left)}, ${childToString(format)})")
                case None ⇒
                    Some(s"PARSEDATETIME(${childToString(left)})")
            }

        case Quarter(date) ⇒
            Some(s"QUARTER(${childToString(date)})")

        case Second(date, _) ⇒
            Some(s"SECOND(${childToString(date)})")

        case WeekOfYear(date) ⇒
            Some(s"WEEK(${childToString(date)})")

        case Year(date) ⇒
            Some(s"YEAR(${childToString(date)})")

        case _ ⇒
            None
    }
}
