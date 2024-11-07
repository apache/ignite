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

import java.net.URL;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandInvoker;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.spring.IgniteSpringHelperImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.ssl.SslContextFactory;
import org.springframework.context.ApplicationContext;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public abstract class AbstractCliCommandInvoker<A extends IgniteDataTransferObject> extends CommandInvoker<A>
    implements AutoCloseable {
    /** */
    protected final ConnectionAndSslParameters<A> args;

    /** */
    protected final Function<String, char[]> pwdReader;

    /** */
    protected AbstractCliCommandInvoker(
        Command<A, ?> cmd,
        ConnectionAndSslParameters<A> args,
        Function<String, char[]> pwdReader
    ) {
        super(cmd, args.commandArg(), null);

        this.args = args;
        this.pwdReader = pwdReader;
    }

    /** @return Message text to show user for. {@code null} means that confirmantion is not required. */
    public String confirmationPrompt() {
        return cmd.confirmationPrompt(arg);
    }

    /** */
    public abstract <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception;

    /**
     * @param args Commond args.
     * @return Ssl support factory.
     */
    protected Factory<SSLContext> createSslSupportFactory(ConnectionAndSslParameters args) throws IgniteCheckedException {
        if (!F.isEmpty(args.sslKeyStorePath()) && !F.isEmpty(args.sslFactoryConfigPath())) {
            throw new IllegalArgumentException("Incorrect SSL configuration. " +
                "SSL factory config path should not be specified simultaneously with other SSL options like keystore path.");
        }

        if (!F.isEmpty(args.sslFactoryConfigPath())) {
            URL springCfg = IgniteUtils.resolveSpringUrl(args.sslFactoryConfigPath());

            ApplicationContext ctx = IgniteSpringHelperImpl.applicationContext(springCfg);

            return (Factory<SSLContext>)ctx.getBean(Factory.class);
        }

        SslContextFactory factory = new SslContextFactory();

        if (args.sslProtocol().length > 1)
            factory.setProtocols(args.sslProtocol());
        else
            factory.setProtocol(args.sslProtocol()[0]);

        factory.setKeyAlgorithm(args.sslKeyAlgorithm());
        factory.setCipherSuites(args.getSslCipherSuites());
        factory.setKeyStoreFilePath(args.sslKeyStorePath());

        if (args.sslKeyStorePassword() != null)
            factory.setKeyStorePassword(args.sslKeyStorePassword());
        else {
            char[] keyStorePwd = pwdReader.apply("SSL keystore password: ");

            args.sslKeyStorePassword(keyStorePwd);
            factory.setKeyStorePassword(keyStorePwd);
        }

        factory.setKeyStoreType(args.sslKeyStoreType());

        if (F.isEmpty(args.sslTrustStorePath()))
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        else {
            factory.setTrustStoreFilePath(args.sslTrustStorePath());

            if (args.sslTrustStorePassword() != null)
                factory.setTrustStorePassword(args.sslTrustStorePassword());
            else {
                char[] trustStorePwd = pwdReader.apply("SSL truststore password: ");

                args.sslTrustStorePassword(trustStorePwd);
                factory.setTrustStorePassword(trustStorePwd);
            }

            factory.setTrustStoreType(args.sslTrustStoreType());
        }

        return factory;
    }
}
