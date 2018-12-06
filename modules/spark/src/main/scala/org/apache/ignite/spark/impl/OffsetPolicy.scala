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

import java.sql.Timestamp

import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_OFFSET_FIELD, OPTION_OFFSET_POLICY}
import org.apache.spark.sql.execution.streaming.{SerializedOffset, Offset => OffsetV1}
import org.apache.spark.sql.sources.v2.reader.streaming.Offset

import scala.collection.JavaConversions

trait OffsetPolicy {
    def getOffset: Option[Offset]

    def getQuery(start: Option[OffsetV1], end: OffsetV1, fields: Seq[String]): (String, List[Any])
}

object OffsetPolicy {
    def apply(tblName: String, cacheName: String, ignite: Ignite, params: Map[String, String]): OffsetPolicy = {
        val plcName = params.getOrElse(
            OPTION_OFFSET_POLICY,
            throw new IllegalArgumentException(
                s"Option '$OPTION_OFFSET_POLICY' must be specified when using '$FORMAT_IGNITE' as a streaming source."
            )
        )

        plcName.toUpperCase match {
            case "INCREMENTAL" => new IncrementalOffsetPolicy[Long](tblName, cacheName, ignite, params)
            case "TIMESTAMP" => new IncrementalOffsetPolicy[Timestamp](tblName, cacheName, ignite, params)
            case _ => throw new IllegalArgumentException(s"Offset policy '$plcName' is not supported.")
        }
    }
}

private case class IncrementalOffset(off: Any) extends Offset {
    override def json(): String = off match {
        case _: Number => s"{${IncrementalOffset.INC_NAME}: $off}"
        case _: Timestamp => s"{${IncrementalOffset.TS_NAME}: '$off'}"
        case _ => throw new IllegalArgumentException(s"Unsupported offset policy type '${off.getClass}'")
    }
}

private object IncrementalOffset {
    val INC_NAME = "incremental"
    val TS_NAME = "timestamp"

    def apply(off: OffsetV1): IncrementalOffset = {
        off match {
            case incOff: IncrementalOffset => incOff
            case serOff: SerializedOffset => fromJson(serOff.json)
            case _ => throw new IllegalArgumentException(s"Offset '$off' is of unsupported typed.")
        }
    }

    private def fromJson(json: String): IncrementalOffset = {
        val kv = json.replaceAll("[\\s{}']", "").split(":")

        if (kv.size != 2)
            throw new IllegalArgumentException(s"Malformed incremental offset JSON: $json")

        kv(0) match {
            case INC_NAME => new IncrementalOffset(kv(1).toLong)
            case TS_NAME => new IncrementalOffset(Timestamp.valueOf(kv(1)))
            case _ => throw new IllegalArgumentException(s"Incremental offset type ${kv(0)} is not supported.")
        }
    }
}

private class IncrementalOffsetPolicy[T](
    tblName: String,
    cacheName: String,
    ignite: Ignite,
    params: Map[String, String]
) extends OffsetPolicy {
    private val fldName = params.getOrElse(
        OPTION_OFFSET_FIELD,
        throw new IllegalArgumentException(
            s"Option '$OPTION_OFFSET_FIELD' must be specified when using '$FORMAT_IGNITE' as a " +
                "streaming source with incremental offset policy."
        )
    )

    override def getOffset: Option[Offset] = {
        val qry = new SqlFieldsQuery(s"SELECT MAX($fldName) FROM $tblName")
        val off = ignite.cache(cacheName).query(qry).getAll
        off.size() match {
            case 0 => None
            case _ => off.get(0).get(0) match {
                case null => None
                case n => Some(IncrementalOffset(n.asInstanceOf[T]))
            }
        }
    }

    override def getQuery(start: Option[OffsetV1], end: OffsetV1, fields: Seq[String]): (String, List[Any]) = {
        val qry = (
            s"SELECT ${String.join(",", JavaConversions.seqAsJavaList(fields))} FROM $tblName WHERE $fldName <= ?",
            List(IncrementalOffset.apply(end).off)
        )

        start match {
            case Some(s) => (qry._1 + s" AND $fldName > ?", qry._2 :+ IncrementalOffset.apply(s).off)
            case None => qry
        }
    }
}