package org.apache.spark.sql.ignite

import org.apache.ignite.Ignite
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.internal.SharedState

/**
  */
class IgniteSharedState(ignite: Ignite, sparkContext: SparkContext) extends SharedState(sparkContext) {
    override lazy val externalCatalog: ExternalCatalog = new IgniteExternalCatalog(Some(ignite))
}
