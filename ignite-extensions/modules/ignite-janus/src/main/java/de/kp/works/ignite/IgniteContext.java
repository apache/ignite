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
 * 
 * 
 */

import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.java.JavaLogger;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import de.kp.works.janus.IgniteStore;
import jline.internal.Log;

public class IgniteContext {
	
	
	public static ConcurrentHashMap<String, IgniteContext> instances = new ConcurrentHashMap<>();
	
	public static JavaLogger logger = new JavaLogger();
	
	/*
	 * Reference to Apache Ignite that is transferred to the key value store to
	 * enable cache operations
	 */
	private Ignite ignite;
	

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_CFG = new ConfigOption<>(GraphDatabaseConfiguration.STORAGE_NS,"cfg",
            "The config file path of ignite.",
            ConfigOption.Type.LOCAL, String.class);
    
    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_IGNITE_NAME = new ConfigOption<>(GraphDatabaseConfiguration.STORAGE_NS,"namespace",
            "The config name of ignite.",
            ConfigOption.Type.LOCAL, String.class);

	private IgniteContext(IgniteConfiguration config) {
		instances.put(config.getIgniteInstanceName(), this);
		ignite = Ignition.getOrStart(config);
	}

	private IgniteContext(String cfg,String name) {
		instances.put(name, this);
		// ignite already started
		if(Ignition.allGrids().size()>0) {
			try {
				if (name == null && Ignition.allGrids().size()==1)
					ignite = Ignition.allGrids().get(0);
				else			
					ignite = Ignition.ignite(name);		
			
			}
			catch(IgniteIllegalStateException e) {
				if(name==null || name.isBlank()) {
					name = "ignite-bankend.cfg";
				}
				IgniteConfiguration config = Ignition.loadSpringBean(cfg, name);				
				ignite = Ignition.start(config);
			}
		}
		else {
			// ignite not started
			ignite = Ignition.start(cfg);
		}
	}	

	public static IgniteContext getInstance(String cfg,String name) {
		IgniteContext instance = instances.get(name);
		if (instance == null) {
			instance = new IgniteContext(cfg,name);
		}
		else {
			Log.warn("ignite instance "+name+" already use as other janus graph backend");
		}
		return instance;
	}

	

	public static IgniteContext getInstance(IgniteConfiguration config) {
		IgniteContext instance = instances.get(config.getIgniteInstanceName());
		if (instance == null)
			instance = new IgniteContext(config);
		else {
			Log.warn("ignite instance "+config.getIgniteInstanceName()+" already use as other janus graph backend");
		}
		return instance;
	}

	public Ignite getIgnite() {
		return ignite;
	}

}
