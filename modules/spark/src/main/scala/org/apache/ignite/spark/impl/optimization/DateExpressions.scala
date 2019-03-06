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
