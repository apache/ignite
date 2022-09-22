package de.kp.works.ignite

import de.kp.works.ignite.graph.ElementType
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}

import java.util

object IgniteUtil {

  def createCacheIfNotExists(ignite: Ignite, table: String, cfg: CacheConfiguration[String, BinaryObject]): Unit = {
    val exists: Boolean = ignite.cacheNames.contains(table)
    if (!exists) ignite.createCache(cfg)
  }

  def getOrCreateCache(ignite: Ignite, table: String, namespace: String): IgniteCache[String, BinaryObject] = {

    val exists: Boolean = ignite.cacheNames.contains(table)
    if (exists) {
      ignite.cache(table)
    }
    else {
      createCache(ignite, table, namespace)
    }
  }

  def createCache(ignite: Ignite, table: String, namespace: String): IgniteCache[String, BinaryObject] = {
    createCache(ignite, table, namespace, CacheMode.REPLICATED)
  }

  def createCache(ignite: Ignite, table: String, namespace: String, cacheMode: CacheMode): IgniteCache[String, BinaryObject] = {
    /*
     * Derive the table type from the provided
     * table name
     */
    val tableType = if (table == namespace + "_" + IgniteConstants.EDGES)
      ElementType.EDGE

    else if (table == namespace + "_" + IgniteConstants.VERTICES)
      ElementType.VERTEX

    else

      throw new Exception("Table '" + table + "' is not supported.")

    val cfg = createCacheCfg(table, tableType, cacheMode)
    ignite.createCache(cfg)

  }

  def createCacheCfg(table: String, tableType: ElementType, cacheMode: CacheMode): CacheConfiguration[String, BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qe = buildQueryEntity(table, tableType)
    val qes = new util.ArrayList[QueryEntity]
    qes.add(qe)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    val cfg = new CacheConfiguration[String, BinaryObject]
    cfg.setName(table)

    cfg.setStoreKeepBinary(false)
    cfg.setIndexedTypes(classOf[String], classOf[BinaryObject])

    cfg.setCacheMode(cacheMode)
    cfg.setQueryEntities(qes)

    cfg
  }

  /**
   * This method supports the creation of different
   * query entities (or cache schemas)
   */
  def buildQueryEntity(table: String, elementType: ElementType): QueryEntity = {

    val qe = new QueryEntity
    /*
     * The key type of the Apache Ignite cache is set to
     * [String], i.e. an independent identity management is
     * used here
     */
    qe.setKeyType("java.lang.String")
    /*
     * The 'table' is used as table name in select statement
     * as well as the name of 'ValueType'
     */
    qe.setValueType(table)
    /*
     * Define fields for the Apache Ignite cache that is
     * used as one of the data backends.
     */
    if (elementType == ElementType.EDGE)
      qe.setFields(buildEdgeFields)

    else if (elementType == ElementType.VERTEX)
      qe.setFields(buildVertexFields())

    else
      throw new Exception("Table '" + table + "' is not supported.")

    qe
  }

  def buildEdgeFields: util.LinkedHashMap[String, String] = {

    val fields = new util.LinkedHashMap[String, String]
    /*
     * The edge identifier used by TinkerPop to
     * identify an equivalent of a data row
     */
    fields.put(IgniteConstants.ID_COL_NAME, "java.lang.String")
    /*
     * The edge identifier type to reconstruct the
     * respective value. IgniteGraph supports [Long]
     * as well as [String] as identifier.
     */
    fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String")
    /*
     * The edge label used by TinkerPop and IgniteGraph
     */
    fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String")
    /*
     * The `TO` vertex description
     */
    fields.put(IgniteConstants.TO_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.TO_TYPE_COL_NAME, "java.lang.String")
    /*
     * The `FROM` vertex description
     */
    fields.put(IgniteConstants.FROM_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.FROM_TYPE_COL_NAME, "java.lang.String")
    /*
             * The timestamp this cache entry has been created.
             */
    fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The timestamp this cache entry has been updated.
     */
    fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The property section of this cache entry
     */
    fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String")
    /*
     * The serialized property value
     */
    fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String")
    fields
  }

  def buildVertexFields(): util.LinkedHashMap[String, String] = {

    val fields: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
    /*
     * The vertex identifier used by TinkerPop to identify
     * an equivalent of a data row
     */
    fields.put(IgniteConstants.ID_COL_NAME, "java.lang.String")
    /*
     * The vertex identifier type to reconstruct the
     * respective value. IgniteGraph supports [Long]
     * as well as [String] as identifier.
     */
    fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String")
    /*
     * The vertex label used by TinkerPop and IgniteGraph
     */
    fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String")
    /*
     * The timestamp this cache entry has been created.
     */
    fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The timestamp this cache entry has been updated.
     */
    fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The property section of this cache entry
     */
    fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String")
    /*
     * The serialized property value
     */
    fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String")
    fields
  }

}
