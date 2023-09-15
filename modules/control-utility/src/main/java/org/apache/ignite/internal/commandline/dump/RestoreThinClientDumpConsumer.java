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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class RestoreThinClientDumpConsumer implements DumpConsumer {
    /** */
    private final IgniteClient cli;

    /** Reader logger. */
    private final IgniteLogger log;

    /** */
    public RestoreThinClientDumpConsumer(ClientConfiguration cfg) {
        cli = TcpIgniteClient.start(cfg);

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

    }

    /** {@inheritDoc} */
    @Override public void stop() {

    }
}
