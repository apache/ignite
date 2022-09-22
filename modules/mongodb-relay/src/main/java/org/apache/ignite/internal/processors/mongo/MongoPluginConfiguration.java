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

package org.apache.ignite.internal.processors.mongo;

import org.apache.ignite.plugin.PluginConfiguration;

/**
 * Configuration of ML plugin that defines which ML inference services should be start up on Ignite startup.
 */
public class MongoPluginConfiguration implements PluginConfiguration {
    private boolean isWithBinaryStorage  = false;
    
    /** Host. */
    private String host = "0.0.0.0";
    
    private int port = 27018;

    /** Number of backups in mongodb storage cache. */
    private int mdlStorageBackups;

    /** Number of backups in mongodb descriptor storage cache. */
    private int mdlDescStorageBackups;
    
    private boolean partitioned = true;

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
    public int getMdlStorageBackups() {
        return mdlStorageBackups;
    }

    /** */
    public void setMdlStorageBackups(int mdlStorageBackups) {
        this.mdlStorageBackups = mdlStorageBackups;
    }

    /** */
    public int getMdlDescStorageBackups() {
        return mdlDescStorageBackups;
    }

    /** */
    public void setMdlDescStorageBackups(int mdlDescStorageBackups) {
        this.mdlDescStorageBackups = mdlDescStorageBackups;
    }

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public boolean isPartitioned() {
		return partitioned;
	}

	public void setPartitioned(boolean partitioned) {
		this.partitioned = partitioned;
	}
}
