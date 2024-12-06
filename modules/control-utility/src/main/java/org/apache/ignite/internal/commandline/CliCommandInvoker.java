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

package org.apache.ignite.internal.commandline;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientNodeStateBeforeStart;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CLIENT_ATTR;

/**
 * Adapter of new management API command for {@code control.sh} execution flow.
 */
public class CliCommandInvoker<A extends IgniteDataTransferObject> extends AbstractCliCommandInvoker<A> {
    /** Client. */
    private final IgniteClient client;

    /** @param cmd Command to execute. */
    public CliCommandInvoker(
        Command<A, ?> cmd,
        ConnectionAndSslParameters<A> args,
        Function<String, char[]> pwdReader
    ) throws IgniteCheckedException {
        super(cmd, args, pwdReader);

        ClientConfiguration cfg = clientConfiguration(args);

        if (cmd instanceof BeforeNodeStartCommand) {
            cfg.setUserAttributes(F.asMap(MANAGEMENT_CLIENT_ATTR, Boolean.TRUE.toString()));
            cfg.setAutoBinaryConfigurationEnabled(false);
        }

        client = Ignition.startClient(cfg);
    }

    /** {@inheritDoc} */
    @Override protected GridClientNode defaultNode() {
        return CommandUtils.clusterToClientNode(client.cluster().forOldest().node());
    }

    /** {@inheritDoc} */
    @Override protected @Nullable IgniteClient client() {
        return client;
    }

    /** {@inheritDoc} */
    @Override public <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception {
        return ((BeforeNodeStartCommand<A, R>)cmd).execute(new GridClientNodeStateBeforeStart() {
            @Override public void stopWarmUp() {
                ((TcpIgniteClient)client).stopWarmUp();
            }
        }, arg, printer);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        client.close();
    }

    /**
     * @param args Common arguments.
     * @return Thin client configuration to connect to cluster.
     * @throws IgniteCheckedException If error occur.
     */
    private ClientConfiguration clientConfiguration(
        ConnectionAndSslParameters args
    ) throws IgniteCheckedException {
        ClientConfiguration clientCfg = new ClientConfiguration();

        clientCfg.setAddresses(args.host() + ":" + args.port());

        if (!F.isEmpty(args.userName())) {
            clientCfg.setUserName(args.userName());
            clientCfg.setUserPassword(args.password());
        }

        if (!F.isEmpty(args.sslKeyStorePath()) || !F.isEmpty(args.sslFactoryConfigPath())) {
            clientCfg.setSslContextFactory(createSslSupportFactory(args));
            clientCfg.setSslMode(SslMode.REQUIRED);
        }

        clientCfg.setClusterDiscoveryEnabled(false);

        return clientCfg;
    }
}
