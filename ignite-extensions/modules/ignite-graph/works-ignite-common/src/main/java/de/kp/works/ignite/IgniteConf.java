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
   * Configure default java logger which leverages file
   * config/java.util.logging.properties
   */
  public static JavaLogger logger = new JavaLogger();  
  
  
  /*
   * This implementation expects that the configuration
   * file is located resources/META-INF as in this case,
   * Apache Ignite automatically detects files in this
   * folder
   */
  public String file = "config/graph-config.xml";
  
  public String igniteName = "graph";
  
  
  public IgniteConf(String cfg,String name) {
	  if(cfg!=null && !cfg.isBlank())
		  this.file = cfg;
	  this.igniteName = name;
  }

  public String fromFile() {	return file;	}

  public IgniteConfiguration fromConfig() {
	  IgniteConfiguration config = null;
	  String[] cfgs = {igniteName, igniteName+".cfg", "graph-"+igniteName, "default", ""};
	  for(String cfg: cfgs) {
		  try {
			  config = Ignition.loadSpringBean(fromFile(), cfg);
			  config.setGridLogger(logger);
			  return config;
		  }
		  catch(Exception e) {
			  logger.warning("try load config from " + file+ " beanName = " + cfg, e);
		  }
	  }
	  return null;
	    
  }
}
