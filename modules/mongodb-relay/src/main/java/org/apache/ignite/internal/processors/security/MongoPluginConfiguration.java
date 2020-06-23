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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.plugin.PluginConfiguration;

/**
 * Configuration of ML plugin that defines which ML inference services should be start up on Ignite startup.
 */
public class MongoPluginConfiguration implements PluginConfiguration {
    private boolean isWithBinaryStorage  = false;
    
    /** Host. */
    private String host = "127.0.0.1";
    
    private int port = 27018;

    /** Number of backups in mongodb storage cache. */
    private Integer mdlStorageBackups;

    /** Number of backups in mongodb descriptor storage cache. */
    private Integer mdlDescStorageBackups;

    /** */
    public boolean isWithBinaryStorage() {
        return isWithBinaryStorage;
    }

    /** */
    public void setWithBinaryStorage(boolean withMdlStorage) {
        this.isWithBinaryStorage = withMdlStorage;
    }

    /** */
    public int getPort() {
        return port;
    }

    /** */
    public void setPort(int port) {
        this.port = port;
    }

    /** */
    public Integer getMdlStorageBackups() {
        return mdlStorageBackups;
    }

    /** */
    public void setMdlStorageBackups(Integer mdlStorageBackups) {
        this.mdlStorageBackups = mdlStorageBackups;
    }

    /** */
    public Integer getMdlDescStorageBackups() {
        return mdlDescStorageBackups;
    }

    /** */
    public void setMdlDescStorageBackups(Integer mdlDescStorageBackups) {
        this.mdlDescStorageBackups = mdlDescStorageBackups;
    }

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}
