package de.kp.works.ignite

import org.apache.ignite.configuration.{DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.logger.java.JavaLogger

/**
 * Apache Ignite Spark ships with two different
 * approaches to access configuration information:
 *
 * When starting an Apache Ignite node, an instance
 * of [IgniteConfiguration] is required, while Ignite
 * dataframes requires the path to a configuration file.
 */
object IgniteConf {
  /*
   * This implementation expects that the configuration
   * file is located resources/META-INF as in this case,
   * Apache Ignite automatically detects files in this
   * folder
   */
  val file = "ignite-config.xml"
  /*
   * Configure default java logger which leverages file
   * config/java.util.logging.properties
   */
  val logger = new JavaLogger()
  /*
   * The current Ignite context is configured with the
   * default configuration (except 'marshaller')
   */
  val config = new IgniteConfiguration()
  config.setGridLogger(logger)

  val ds = new DataStorageConfiguration()
  /*
   * Lessons learned: at least 750 MB
   */
  ds.setSystemRegionMaxSize(2L * 1024 * 1024 * 1024)
  config.setDataStorageConfiguration(ds)

  def fromFile: String = file

  def fromConfig: IgniteConfiguration = config

}
