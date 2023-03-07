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
package de.kp.works.ignite.gremlin.plugin;


import org.apache.ignite.plugin.PluginConfiguration;

/**
 * Configuration of ML plugin that defines which ML inference services should be start up on Ignite startup.
 */
public class GremlinPluginConfiguration implements PluginConfiguration {
    private boolean isWithBinaryStorage  = true;
    private boolean isPersistenceEnabled = false;
    
    /** cfg file. */
    private String gremlinServerCfg = "config/gremlin-server/gremlin-server-ignite.yaml";

	public boolean isWithBinaryStorage() {
		return isWithBinaryStorage;
	}

	public void setWithBinaryStorage(boolean isWithBinaryStorage) {
		this.isWithBinaryStorage = isWithBinaryStorage;
	}

	public String getGremlinServerCfg() {
		return gremlinServerCfg;
	}

	public void setGremlinServerCfg(String gremlinServerCfg) {
		this.gremlinServerCfg = gremlinServerCfg;
	}

	public boolean isPersistenceEnabled() {
		return isPersistenceEnabled;
	}

	public void setPersistenceEnabled(boolean isPersistenceEnabled) {
		this.isPersistenceEnabled = isPersistenceEnabled;
	}
    
}
