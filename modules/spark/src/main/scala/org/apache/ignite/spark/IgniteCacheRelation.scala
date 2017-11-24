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

package org.apache.ignite.spark

import java.lang.reflect.Modifier

import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.spark.impl.IgniteSqlRDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import javax.cache.Cache

import org.apache.ignite.IgniteException
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.annotation.tailrec

/**
  * Relation to provide data from regular key-value cache.
  */
class IgniteCacheRelation[K, V](ic: IgniteContext, cache: String, keyClass: Class[_], valueClass: Class[_],
    val keepBinary: Boolean)(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {

    /**
      * @return Schema of data stored in cache.
      */
    override def schema: StructType =
        if (cacheExists(ic.ignite(), cache))
            IgniteCacheRelation.schema(keyClass, valueClass)
        else
            throw new IgniteException(s"Unknown cache '$cache'")

    /**
      * @param columns Columns to select.
      * @param filters Filters to apply.
      * @return Apache Ignite RDD implementation.
      */
    override def buildScan(columns: Array[String], filters: Array[Filter]): IgniteSqlRDD[Row, Cache.Entry[K, V], K, V] =
        if (cacheExists(ic.ignite(), cache)) {
            val qry = enclose(keepBinary) { keepBinary ⇒
                val qry = new ScanQuery[K, V]()

                //Building filter that will be executed on remote Ignite node.
                qry.setFilter(new IgniteBiPredicate[K, V] {
                    def checkClause(k: K, v: V, clause: Filter): Boolean = {
                        def compare(attr: String, filterVal: Any, comparator: (Int) ⇒ Boolean) = {
                            val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                            if (!field.isInstanceOf[Comparable[_]])
                                throw new IgniteException(s"$field is not Comparable and can't be used in this clause")

                            field != null && comparator(field.asInstanceOf[Comparable[Any]].compareTo(filterVal))
                        }

                        clause match {
                            case EqualTo(attr, filterVal) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                field != null || field.equals(filterVal)

                            case EqualNullSafe(attr, filterVal) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                field == null || field.equals(filterVal)

                            case GreaterThan(attr, filterVal) ⇒ compare(attr, filterVal, (x) ⇒ x > 0)

                            case GreaterThanOrEqual(attr, filterVal) ⇒ compare(attr, filterVal, (x) ⇒ x >= 0)

                            case LessThan(attr, filterVal) ⇒ compare(attr, filterVal, (x) ⇒ x < 0)

                            case LessThanOrEqual(attr, filterVal) ⇒ compare(attr, filterVal, (x) ⇒ x <= 0)

                            case In(attr, filterVals) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                field != null && filterVals.exists(fv ⇒ field.equals(fv))

                            case IsNull(attr) ⇒ IgniteCacheRelation.fieldValue(attr, k, v, keepBinary) == null

                            case IsNotNull(attr) ⇒ IgniteCacheRelation.fieldValue(attr, k, v, keepBinary) != null

                            case And(left, right) ⇒ checkClause(k, v, left) && checkClause(k, v, right)

                            case Or(left, right) ⇒ checkClause(k, v, left) || checkClause(k, v, right)

                            case Not(child) ⇒ !checkClause(k, v, child)

                            case StringStartsWith(attr, filterVal) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                if (!field.isInstanceOf[String])
                                    throw new IgniteException(s"$field is not String and can't be used in this clause")

                                field != null && field.asInstanceOf[String].startsWith(filterVal)

                            case StringEndsWith(attr, filterVal) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                if (!field.isInstanceOf[String])
                                    throw new IgniteException(s"$field is not String and can't be used in this clause")

                                field != null && field.asInstanceOf[String].endsWith(filterVal)

                            case StringContains(attr, filterVal) ⇒
                                val field = IgniteCacheRelation.fieldValue(attr, k, v, keepBinary)

                                if (!field.isInstanceOf[String])
                                    throw new IgniteException(s"$field is not String and can't be used in this clause")

                                field != null && field.asInstanceOf[String].contains(filterVal)
                        }
                    }

                    override def apply(key: K, value: V): Boolean =
                        filters.forall(checkClause(key, value, _))
                })

                qry
            }

            //Converter from Entry to RDD.
            val conv = enclose((schema, keepBinary)) { case (schema, keepBinary) ⇒
                (r: Cache.Entry[K, V]) ⇒ {
                    val values =
                        columns.map(name ⇒ IgniteCacheRelation.fieldValue(name, r.getKey, r.getValue, keepBinary))

                    new GenericRowWithSchema(values.map(IgniteRDD.convertIfNeeded), schema)
                }
            }

            IgniteSqlRDD[Row, Cache.Entry[K, V], K, V](ic, cache, null, qry, conv, keepBinary)
        } else
            throw new IgniteException(s"Unknown cache '$cache'")

    override def toString = s"IgniteCacheRelation[cache=$cache,key=${keyClass.getSimpleName},value=${valueClass.getSimpleName}]"
}

object IgniteCacheRelation {
    def apply[K, V](ic: IgniteContext, cache: String, keyClass: Class[_], valueClass: Class[_], keepBinary: Boolean,
        sqlContext: SQLContext): IgniteCacheRelation[K, V] =
        new IgniteCacheRelation[K, V](ic, cache, keyClass, valueClass, keepBinary)(sqlContext)

    /**
      * @param v Class to check.
      * @return `true` if class is simple.
      */
    def isSimpleClass(v: Class[_]): Boolean =
        IgniteUtils.isPrimitiveOrWrapper(v) || IgniteUtils.isJdk(v)

    /**
      * Returns value of field from key or value.
      * If name == `value` returns value.
      * If name == `key` returns key.
      * If name == `value.XXX` returns XXX field from `value`.
      * If name == `key.XXX` returns XXX field from `key`.
      *
      * If `keepBinary` is true than key or value has to be `BinaryObject`.
      *
      * @param name Field name.
      * @param key Key.
      * @param value Value.
      * @param keepBinary KeepBinary flag.
      * @tparam K Key class.
      * @tparam V Value class.
      * @return Value of field from key or value.
      */
    def fieldValue[K, V](name: String, key: K, value: V, keepBinary: Boolean): Any =
        if (name == "value") {
            if (value == null || IgniteCacheRelation.isSimpleClass(value.getClass) || !keepBinary)
                value
            else
                value.asInstanceOf[BinaryObject].deserialize
        }
        else if (name == "key") {
            if (IgniteCacheRelation.isSimpleClass(key.getClass) || !keepBinary)
                key
            else
                key.asInstanceOf[BinaryObject].deserialize
        }
        else {
            val (obj, fieldName) =
                if (name.startsWith("value"))
                    (value, name.substring(6))
                else
                    (key, name.substring(4))

            if (obj == null)
                null
            else if (keepBinary)
                obj.asInstanceOf[BinaryObject].field(fieldName)
            else {
                val field = obj.getClass.getField(fieldName)

                field.setAccessible(true)

                field.get(obj)
            }
        }

    /**
      * @param keyClass Key class.
      * @param valueClass Value class
      * @return Schema from classes.
      */
    def schema(keyClass: Class[_], valueClass: Class[_]): StructType =
        StructType(fields("key", keyClass) ++ fields("value", valueClass))

    /**
      * @param prefix Prefix for names of fields of provided class
      * @param v Class to scan for fields
      * @return Sequence of fields class contains. Is V is simple class - return StructField for v itself.
      */
    private def fields(prefix: String, v: Class[_]): Seq[StructField] = {
        @tailrec
        def fields0(prefix: String, v: Class[_], curr: Seq[StructField]): Seq[StructField] =
            if (v == classOf[Object])
                curr
            else if (IgniteCacheRelation.isSimpleClass(v)) {
                curr :+ StructField(name = prefix, dataType = IgniteRDD.dataType(v.getName, prefix), nullable = false,
                    metadata = Metadata.empty)
            }
            else {
                val classFields = v.getDeclaredFields.filter { f ⇒
                    val mod = f.getModifiers

                    !Modifier.isStatic(mod) && !Modifier.isTransient(mod);
                }

                val newStructFields = classFields.map { f ⇒
                    val fieldName = prefix + "." + f.getName
                    StructField(
                        name = fieldName,
                        dataType = IgniteRDD.dataType(f.getType.getName, fieldName),
                        nullable = true,
                        metadata = Metadata.empty)
                }

                fields0(prefix, v.getSuperclass, curr ++ newStructFields)
            }

        fields0(prefix, v, Seq.empty)
    }
}
