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

package org.apache.ignite.internal.commandline.dump;

import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class RestoreThinClientDumpConsumer implements DumpConsumer {
    /** */
    private final ClientConfiguration cfg;

    /** */
    private IgniteClient cli;

    /** Reader logger. */
    private final IgniteLogger log;

    /** */
    public RestoreThinClientDumpConsumer(ClientConfiguration cfg) {
        this.cfg = cfg;

        try {
            IgniteConfiguration fakeCfg = new IgniteConfiguration();

            U.initWorkDir(fakeCfg);

            log = U.initLogger(fakeCfg, "ignite-dump-reader");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        cli = TcpIgniteClient.start(cfg);

    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {

    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {

    }

    /** {@inheritDoc} */
    @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {

    }

    /** {@inheritDoc} */
    @Override public void onData(Iterator<DumpEntry> data) {

    }

    /** {@inheritDoc} */
    @Override public void stop() {
        cli.close();
    }
}
