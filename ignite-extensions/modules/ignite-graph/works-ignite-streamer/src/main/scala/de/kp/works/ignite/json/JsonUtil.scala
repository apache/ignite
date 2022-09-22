package de.kp.works.ignite.json
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.gson.JsonObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.security.MessageDigest

import scala.collection.JavaConversions._
import scala.collection.mutable

object JsonUtil {

  private val MD5 = MessageDigest.getInstance("MD5")

  def json2Row(jsonObject:JsonObject, schema:StructType):Row = {

    val keyparts = mutable.ArrayBuffer.empty[String]
    /*
     * NOTE: The schema contains the primary key as
     * first field; in order to extract the values,
     * the tail of the fields must be used.
     */
    var values = schema.fields.tail.map(field => {

      val fieldName = field.name
      val fieldType = field.dataType

      fieldType match {
        case ArrayType(LongType, true) =>
          val value = getLongArray(jsonObject, fieldName, field.nullable)

          keyparts += value.mkString("#")
          value
        case ArrayType(LongType, false) =>
          val value = getLongArray(jsonObject, fieldName, field.nullable)

          keyparts += value.mkString("#")
          value
        case ArrayType(StringType, true) =>
          val value = getStringArray(jsonObject, fieldName, field.nullable)

          keyparts += value.mkString("#")
          value
        case ArrayType(StringType, false) =>
          val value = getStringArray(jsonObject, fieldName, field.nullable)

          keyparts += value.mkString("#")
          value
        case BooleanType =>
          val value = getBoolean(jsonObject, fieldName, field.nullable)

          keyparts += value.toString
          value
        case DoubleType =>
          val value = getDouble(jsonObject, fieldName, field.nullable)

          keyparts += value.toString
          value
        case IntegerType =>
          val value = getInt(jsonObject, fieldName, field.nullable)

          keyparts += value.toString
          value
        case LongType =>
          val value = getLong(jsonObject, fieldName, field.nullable)

          keyparts += value.toString
          value
        case StringType =>
          val value = getString(jsonObject, fieldName, field.nullable)

          keyparts += value
          value

        case _ => throw new Exception(s"Data type `$fieldType.toString` is not supported.")
      }

    }).toSeq

    val serialized = keyparts.mkString("|")
    val key = MD5.digest(serialized.getBytes).toString

    values = Seq(key) ++ values
    Row.fromSeq(values)

  }

  def getBoolean(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Boolean = {

    try {
      jsonObject.get(fieldName).getAsBoolean

    } catch {
      case _:Throwable =>
        if (nullable) false
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getDouble(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Double = {

    try {
      jsonObject.get(fieldName).getAsDouble

    } catch {
      case _:Throwable =>
        if (nullable) Double.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getInt(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Int = {

    try {
      jsonObject.get(fieldName).getAsInt

    } catch {
      case _:Throwable =>
        if (nullable) Int.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getLong(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Long = {

    try {
      jsonObject.get(fieldName).getAsLong

    } catch {
      case _:Throwable =>
        if (nullable) Long.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getString(jsonObject:JsonObject, fieldName:String, nullable:Boolean):String = {

    try {
      jsonObject.get(fieldName).getAsString

    } catch {
      case _:Throwable =>
        if (nullable) ""
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getLongArray(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Array[Long] = {

    try {
      jsonObject.get(fieldName).getAsJsonArray
        .map(json => json.getAsLong)
        .toArray

    } catch {
      case _:Throwable =>
        if (nullable) Array.empty[Long]
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  def getStringArray(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Array[String] = {

    try {
      jsonObject.get(fieldName).getAsJsonArray
        .map(json => json.getAsString)
        .toArray

    } catch {
      case _:Throwable =>
        if (nullable) Array.empty[String]
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

}
