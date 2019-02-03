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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
  * Utility class for building SQL queries.
  */
private[impl] object QueryUtils extends Logging {
    /**
      * Builds `where` part of SQL query.
      *
      * @param filters Filter to apply.
      * @return Tuple contains `where` string and `List[Any]` of query parameters.
      */
    def compileWhere(filters: Seq[Filter]): (String, List[Any]) =
        filters.foldLeft(("", List[Any]()))(buildSingleClause)

    /**
      * Builds `insert` query for provided table and schema.
      *
      * @param tblName Table name.
      * @param tblSchema Schema.
      * @return SQL query to insert data into table.
      */
    def compileInsert(tblName: String, tblSchema: StructType): String = {
        val columns = tblSchema.fields.map(_.name).mkString(",")
        val placeholder = tblSchema.fields.map(_ ⇒ "?").mkString(",")

        val qry = s"INSERT INTO $tblName($columns) VALUES($placeholder)"

        logInfo(qry)

        qry
    }

    /**
      * Builds `drop table` query.
      *
      * @param tblName Table name.
      * @return SQL query to drop table.
      */
    def compileDropTable(tblName: String): String = {
        val qry = s"DROP TABLE ${tblName}"

        logInfo(qry)

        qry
    }

    /**
      * Builds `create table` query.
      *
      * @param schema Schema.
      * @param tblName Table name.
      * @param primaryKeyFields Primary key fields.
      * @param createTblOpts Ignite specific options for table.
      * @return SQL query to create table.
      */
    def compileCreateTable(schema: StructType, tblName: String, primaryKeyFields: Seq[String], createTblOpts: Option[String]): String = {
        val pk = s", PRIMARY KEY (${primaryKeyFields.mkString(",")})"

        val withParams = createTblOpts.map(w ⇒ s"""WITH \"$w\"""").getOrElse("")

        val qry = s"CREATE TABLE $tblName (${schema.map(compileColumn).mkString(", ")} $pk) $withParams"

        logInfo(qry)

        qry
    }

    /**
      * @param field Column.
      * @return SQL query part for column.
      */
    private def compileColumn(field: StructField): String = {
        val col = s"${field.name} ${dataType(field)}"

        if (!field.nullable)
            col + " NOT NULL"
        else
            col
    }

    /**
      * Gets Ignite data type based on type name.
      *
      * @param field Field.
      * @return SQL data type.
      */
    private def dataType(field: StructField): String = field.dataType match {
        case BooleanType ⇒
            "BOOLEAN"

        case ByteType ⇒
            "TINYINT"

        case ShortType ⇒
            "SMALLINT"

        case IntegerType ⇒
            "INT"

        case LongType ⇒
            "BIGINT"

        case FloatType ⇒
            "FLOAT"

        case DoubleType ⇒
            "DOUBLE"

        //For now Ignite doesn't provide correct information about DECIMAL column precision and scale.
        //All we have is default scale and precision.
        //Just replace it with some "common sense" values.
        case decimal: DecimalType if decimal.precision == 10 && decimal.scale == 0 ⇒
            s"DECIMAL(10, 5)"

        case decimal: DecimalType ⇒
            s"DECIMAL(${decimal.precision}, ${decimal.scale})"

        case StringType ⇒
            "VARCHAR"

        case DateType ⇒
            "DATE"

        case TimestampType ⇒
            "TIMESTAMP"

        case _ ⇒
            throw new IgniteException(s"Unsupported data type ${field.dataType}")
    }

    /**
      * Adds single where clause to `state` and returns new state.
      *
      * @param state Current `where` state.
      * @param clause Clause to add.
      * @return `where` with given clause.
      */
    private def buildSingleClause(state: (String, List[Any]), clause: Filter): (String, List[Any]) = {
        val filterStr = state._1

        val params = state._2

        clause match {
            case EqualTo(attr, value) ⇒ (addStrClause(filterStr, s"$attr = ?"), params :+ value)

            case EqualNullSafe(attr, value) ⇒ (addStrClause(filterStr, s"($attr IS NULL OR $attr = ?)"), params :+ value)

            case GreaterThan(attr, value) ⇒ (addStrClause(filterStr, s"$attr > ?"), params :+ value)

            case GreaterThanOrEqual(attr, value) ⇒ (addStrClause(filterStr, s"$attr >= ?"), params :+ value)

            case LessThan(attr, value) ⇒ (addStrClause(filterStr, s"$attr < ?"), params :+ value)

            case LessThanOrEqual(attr, value) ⇒ (addStrClause(filterStr, s"$attr <= ?"), params :+ value)

            case In(attr, values) ⇒ (addStrClause(filterStr, s"$attr IN (${values.map(_ ⇒ "?").mkString(",")})"), params ++ values)

            case IsNull(attr) ⇒ (addStrClause(filterStr, s"$attr IS NULL"), params)

            case IsNotNull(attr) ⇒ (addStrClause(filterStr, s"$attr IS NOT NULL"), params)

            case And(left, right) ⇒
                val leftClause = buildSingleClause(("", params), left)
                val rightClause = buildSingleClause(("", leftClause._2), right)

                (addStrClause(filterStr, s"${leftClause._1} AND ${rightClause._1}"), rightClause._2)

            case Or(left, right) ⇒
                val leftClause = buildSingleClause(("", params), left)
                val rightClause = buildSingleClause(("", leftClause._2), right)

                (addStrClause(filterStr, s"${leftClause._1} OR ${rightClause._1}"), rightClause._2)

            case Not(child) ⇒
                val innerClause = buildSingleClause(("", params), child)

                (addStrClause(filterStr, s"NOT ${innerClause._1}"), innerClause._2)

            case StringStartsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ (value + "%"))

            case StringEndsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value))

            case StringContains(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value + "%"))
        }
    }

    /**
      * Utility method to add clause to sql WHERE string.
      *
      * @param filterStr Current filter string
      * @param clause Clause to add.
      * @return Filter string.
      */
    private def addStrClause(filterStr: String, clause: String) =
        if (filterStr.isEmpty)
            clause
        else
            filterStr + " AND " + clause
}
