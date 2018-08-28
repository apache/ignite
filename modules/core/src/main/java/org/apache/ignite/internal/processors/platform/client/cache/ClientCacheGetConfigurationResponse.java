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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache configuration response.
 */
public class ClientCacheGetConfigurationResponse extends ClientResponse {
    /** Cache configuration. */
    private final CacheConfiguration cfg;

    /** Client version. */
    private final ClientListenerProtocolVersion ver;
    
    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param cfg Cache configuration.
     * @param ver Client version.
     */
    ClientCacheGetConfigurationResponse(long reqId, CacheConfiguration cfg, ClientListenerProtocolVersion ver) {
        super(reqId);

        assert cfg != null;
        assert ver != null;

        this.cfg = cfg;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public void encode(BinaryRawWriterEx writer) {
        super.encode(writer);

        ClientCacheConfigurationSerializer.write(writer, cfg, ver);
    }
}
