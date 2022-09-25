package de.kp.works.ignite;
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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.java.JavaLogger;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class IgniteContext {
	/*
	 * Reference to Apache Ignite that is transferred to the key value store to
	 * enable cache operations
	 */
	private final Ignite ignite;

	private static IgniteContext instance;
	
	public static JavaLogger logger = new JavaLogger();

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_CFG = new ConfigOption<>(GraphDatabaseConfiguration.STORAGE_NS,"cfg",
            "The config file path of ignite.",
            ConfigOption.Type.LOCAL, String.class);

	private IgniteContext(IgniteConfiguration config) {

		if (config == null && Ignition.allGrids().size()==0)
			ignite = Ignition.start();
		else if (config == null && Ignition.allGrids().size()==1)
			ignite = Ignition.allGrids().get(0);
		else if (config == null && Ignition.allGrids().size()>1)
			ignite = Ignition.ignite();
		else {			
			ignite = Ignition.getOrStart(config);
		}
	}

	private IgniteContext(String cfg) {			
		this(fromConfig(cfg));
	}

	public static IgniteConfiguration fromConfig(String cfg) {	  
	  try {
		  IgniteConfiguration config = Ignition.loadSpringBean(cfg, "ignite.cfg");
		  return config;
	  }
	  catch(Exception e) {
		  logger.error("Error load config from "+cfg,e);
	  }
	  return null; 	  
   }
	

	public static IgniteContext getInstance(String cfg) {
		if(cfg==null) {
			cfg = "conf/ignite/backend-config.xml";
		}
		if (instance == null)
			instance = new IgniteContext(cfg);
		return instance;
	}

	

	public static IgniteContext getInstance(IgniteConfiguration config) {
		if (instance == null)
			instance = new IgniteContext(config);
		return instance;
	}

	public Ignite getIgnite() {
		return ignite;
	}

}
