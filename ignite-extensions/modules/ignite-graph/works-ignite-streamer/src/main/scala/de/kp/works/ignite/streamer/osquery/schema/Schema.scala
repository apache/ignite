package de.kp.works.ignite.streamer.osquery.schema
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import org.apache.spark.sql.types._

import java.io.InputStream
import scala.collection.mutable
import scala.io.Source

case class Column(name:String, `type`:DataType)
case class Table(name:String, columns:Seq[Column])

object Schema {

  private val path = "osquery_schema.sql"
  
  private var tables:Option[Map[String, Table]] = None
  private var types:Option[List[(String,DataType)]] = None
  
  private val createTable = "CREATE TABLE ([a-z0-9_]+) \\(([a-zA-z0-9, ]+)\\);".r
  private val alterTable = "ALTER TABLE ([a-z0-9_]+) ADD ([a-zA-z0-9, ]+);".r

  build()

  def main(args:Array[String]):Unit = {

    val names = types.get
      .map{case(cname,ctype) => (cname, 1)}
      .groupBy{case(cname, _) => cname}
      .map{case(name, values) => (name, values.size)}
      .filter{case(name, count) => count > 1}

    println(names)
  }

  /*
   * Method to retrieve all table names
   */
  def getNames: Seq[String] = {

    if (tables.isEmpty)
      Seq.empty[String]

    else
      tables.get.keys.toSeq.sorted

  }
  /*
   * Method to retrieve all tables
   */
  def getTables: Map[String, Table] = {

    if (tables.isEmpty)
      Map.empty[String,Table]

    else
      tables.get

  }
  /*
   * Method to check whether a certain
   * query result refers to a pre-defined
   * table
   */
  def isTable(s:String):Boolean = {
    if (tables.isEmpty) return false
    tables.get.contains(s)
  }
  /*
   * Retrieves the columns of a certain
   * table for more detailed validation
   */
  def getColumns(name:String):Seq[Column] = {

    if (tables.isEmpty) return Seq.empty[Column]
    if (isTable(name)) {
      val table = tables.get(name)
      table.columns

    }
    else null
    
  }
  
  def getColumnMap(name:String):Map[String,DataType] = {

    if (tables.isEmpty) return Map.empty[String,DataType]
    if (isTable(name)) {
            
      val table = tables.get(name)
      table.columns.map(column => (column.name, column.`type`)).toMap

    }
    else null
    
  }
  /**
   * This method expects that field names with the Osquery
   * schemas have a unique data type assigned.
   */
  def getTypes:Map[String, DataType] = {

    if (types.isEmpty) return Map.empty[String,DataType]
    types.get.toMap

  }

  private def build():Unit = {
   
    val buffer = mutable.HashMap.empty[String,Table]

    val stream: InputStream = getClass.getResourceAsStream(s"/$path")

    val lines: Iterator[String] = Source.fromInputStream( stream ).getLines
    lines.foreach(line => {
      
      if (line.startsWith("CREATE TABLE")) {
        
        /*
         * Extract raw data from provided line
         */
        var name:String = null
        var cols:String = null
        
        val clean = line.replaceAll("\"","")        
        createTable.findAllIn(clean).matchData.foreach(m => {          
          name = m.group(1)
          cols = m.group(2)          
        })

        if (name == null || cols == null)
          throw new Exception("[CREATE TABLE] Schema extraction failed.")
        /*
         * Transform columns into structured format
         */
        val columns = cols.split(",").map(chunk => {
          
          val tokens = chunk.trim.split(" ")
          
          val cname = tokens(0).trim
          val ctype = tokens(1).trim match {
            case "BIGINT"   => LongType
            /*
             * With respect to osquery.io, the DATETIME data type
             * is described as TEXT or String
             */
            case "DATETIME" => StringType
            case "DOUBLE"   => DoubleType
            case "INTEGER"  => IntegerType
            case "TEXT"     => StringType
            case _ => throw new Exception(s"Data type `${tokens(1).trim}` is not supported.")
          }
          
          Column(name = cname, `type` = ctype)
          
        })
        
        val table = Table(name = name, columns = columns.toSeq)
        buffer += name -> table
        
      }
      else if (line.startsWith("ALTER TABLE")) {
        /*
         * We expect that the table already exists, and 
         * extract raw data from provided line
         */
        var name:String = null
        var col:String = null
        
        val clean = line.replaceAll("\"","")        
        alterTable.findAllIn(clean).matchData.foreach(m => {          
          name = m.group(1)
          col = m.group(2)          
        })

        if (name == null || col == null)
          throw new Exception("[ALTER TABLE] Schema extraction failed.")
        
        if (!buffer.contains(name))
          throw new Exception(s"[ALTER TABLE] Schema extraction failed, because table `$name` does not exist.")
        
        val tokens = col.trim.split(" ")
          
        val cname = tokens(0).trim
        val ctype = tokens(1).trim match {
          case "BIGINT"   => LongType
          /*
           * With respect to osquery.io, the DATETIME data type
           * is described as TEXT or String
           */
          case "DATETIME" => StringType
          case "DOUBLE"   => DoubleType
          case "INTEGER"  => IntegerType
          case "TEXT"     => StringType
          case _ => throw new Exception(s"Data type `${tokens(1).trim}` is not supported.")
        }
          
        val column = Column(name = cname, `type` = ctype)
        
        val table = buffer(name)
        val columns = table.columns ++ Seq(column)
        
        buffer += name -> table.copy(columns = columns)

      }
    })

    tables = Some(buffer.toMap)
    /*
     * Extract the data types from the provided
     * tables
     */
    types = Some(tables.get.flatMap{ case(_, table) =>
      table.columns.map(column => (column.name, column.`type`))
    }.toList.distinct.sortBy{case(cname, ctype) => cname})

  }
}