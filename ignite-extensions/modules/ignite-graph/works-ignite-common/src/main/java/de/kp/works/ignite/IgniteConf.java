package de.kp.works.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.java.JavaLogger;

/**
 * Apache Ignite Spark ships with two different
 * approaches to access configuration information:
 *
 * When starting an Apache Ignite node, an instance
 * of [IgniteConfiguration] is required, while Ignite
 * dataframes requires the path to a configuration file.
 */
public class IgniteConf {
  /*
   * This implementation expects that the configuration
   * file is located resources/META-INF as in this case,
   * Apache Ignite automatically detects files in this
   * folder
   */
  public static String file = "conf/ignite/graph-config.xml";
  /*
   * Configure default java logger which leverages file
   * config/java.util.logging.properties
   */
  public static JavaLogger logger = new JavaLogger();
  /*
   * The current Ignite context is configured with the
   * default configuration (except 'marshaller')
   */
  public static IgniteConfiguration config = new IgniteConfiguration();
  
  static {
	  config.setGridLogger(logger);
	
	  DataStorageConfiguration ds = new DataStorageConfiguration();
	  /*
	   * Lessons learned: at least 750 MB
	   */
	  ds.setSystemRegionMaxSize(2L * 1024 * 1024 * 1024);
	  config.setDataStorageConfiguration(ds);
  
  }

  public static String fromFile() {	return file;}

  public static IgniteConfiguration fromConfig() { 	
	  try {
		  config = Ignition.loadSpringBean(IgniteConf.fromFile(), "ignite.cfg");
	  }
	  catch(Exception e) {
		  logger.error("Error load config from "+IgniteConf.fromFile(),e);
	  }
	  return config; 	  
  }
}
