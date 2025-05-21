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

package org.apache.ignite.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.commandline.ArgumentParser;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.ConnectionAndSslParameters;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.setupJavaLogger;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.management.api.CommandMBean.INVOKE;
import static org.apache.ignite.internal.management.api.CommandMBean.LAST_RES_METHOD;
import static org.apache.ignite.internal.management.api.CommandUtils.cmdKey;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;

/** Class to check command execution via all available handlers. */
@RunWith(Parameterized.class)
public class GridCommandHandlerFactoryAbstractTest extends GridCommonAbstractTest {
    /** @see JmxCommandHandler */
    public static final String JMX_CMD_HND = "jmx";

    /** @see CliCommandHandler */
    public static final String CLI_CMD_HND = "cli";

    /** */
    public static final Map<String, Function<IgniteLogger, TestCommandHandler>> CMD_HNDS = new HashMap<>();

    static {
        ServiceLoader<TestCommandHandler> svc = ServiceLoader.load(TestCommandHandler.class);

        for (TestCommandHandler hnd : svc) {
            CMD_HNDS.put(hnd.name(), log -> {
                try {
                    return hnd.getClass().getConstructor(IgniteLogger.class).newInstance(log);
                }
                catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                       NoSuchMethodException e) {
                    throw new IgniteException(e);
                }
            });
        }
    }

    /** */
    @Parameterized.Parameter
    public String commandHandler;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return new ArrayList<>(CMD_HNDS.keySet());
    }

    /** */
    protected TestCommandHandler newCommandHandler() {
        return newCommandHandler(null);
    }

    /** Command executor factory. */
    protected TestCommandHandler newCommandHandler(@Nullable IgniteLogger log) {
        if (!CMD_HNDS.containsKey(commandHandler))
            throw new IllegalArgumentException("Unknown handler: " + commandHandler);

        return CMD_HNDS.get(commandHandler).apply(log);
    }

    /** */
    protected boolean cliCommandHandler() {
        return commandHandler.equals(CLI_CMD_HND);
    }

    /** */
    public interface TestCommandHandler {
        /** */
        public <T> T getLastOperationResult();

        /** */
        public void flushLogger();

        /** */
        public int execute(List<String> rawArgs);

        /** */
        public String name();
    }

    /** */
    public static class CliCommandHandler implements TestCommandHandler {
        /** */
        private final CommandHandler hnd;

        /** */
        public CliCommandHandler() {
            this.hnd = new CommandHandler();
        }

        /** */
        public CliCommandHandler(@Nullable IgniteLogger log) {
            this.hnd = log == null ? new CommandHandler() : new CommandHandler(log);
        }

        /** {@inheritDoc} */
        @Override public int execute(List<String> rawArgs) {
            return hnd.execute(rawArgs);
        }

        /** {@inheritDoc} */
        @Override public <T> T getLastOperationResult() {
            return hnd.getLastOperationResult();
        }

        /** {@inheritDoc} */
        @Override public void flushLogger() {
            // Flush all Logger handlers to make log data available to test.
            U.<IgniteLoggerEx>field(hnd, "logger").flush();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return CLI_CMD_HND;
        }
    }

    /** */
    public static class JmxCommandHandler implements TestCommandHandler {
        /** */
        private final IgniteCommandRegistry registry = new IgniteCommandRegistry();

        /** */
        private int port;

        /** */
        private IgniteEx ignite;

        /** */
        private IgniteLoggerEx log;

        /** */
        private Object res;

        /** */
        public JmxCommandHandler() {
            this(null);
        }

        /** */
        public JmxCommandHandler(@Nullable IgniteLogger log) {
            this.log = (IgniteLoggerEx)(log == null ? setupJavaLogger("jmx-invoker", CommandHandler.class) : log);
        }

        /** {@inheritDoc} */
        @Override public int execute(List<String> value) {
            String cmdName = null;

            try {
                ArgumentParser parser = new ArgumentParser(log, registry, null);

                ConnectionAndSslParameters<IgniteDataTransferObject> p = parser.parseAndValidate(value);

                cmdName = toFormattedCommandName(p.cmdPath().peekLast().getClass()).toUpperCase();

                Deque<Command<?, ?>> cmdPath = new ArrayDeque<>(p.cmdPath());

                List<String> grps = new ArrayList<>();

                while (!cmdPath.isEmpty()) {
                    grps.add(cmdKey(
                        cmdPath.pop().getClass(),
                        !cmdPath.isEmpty() ? (Class<? extends CommandsRegistry<?, ?>>)cmdPath.peek().getClass() : null)
                    );
                }

                String name = grps.remove(0);

                Collections.reverse(grps);

                DynamicMBean mbean = getMxBean(
                    ignite(p).context().igniteInstanceName(),
                    "management",
                    grps,
                    name,
                    DynamicMBean.class
                );

                List<String> params = new ArrayList<>();

                Consumer<Field> fldCnsmr = fld -> params.add(toString(U.field(p.commandArg(), fld.getName())));

                visitCommandParams(p.command().argClass(), fldCnsmr, fldCnsmr, (grp, flds) -> flds.forEach(fldCnsmr));

                String[] signature = new String[params.size()];

                Arrays.fill(signature, String.class.getName());

                String out = (String)mbean.invoke(INVOKE, params.toArray(X.EMPTY_OBJECT_ARRAY), signature);

                log.info(out);

                res = mbean.invoke(LAST_RES_METHOD, X.EMPTY_OBJECT_ARRAY, U.EMPTY_STRS);
            }
            catch (MBeanException | ReflectionException e) {
                throw new IgniteException(e);
            }
            catch (Throwable e) {
                log.error("Failed to perform operation.");
                log.error(CommandLogger.errorMessage(e));

                if (X.hasCause(e, IllegalArgumentException.class)) {
                    IllegalArgumentException iae = X.cause(e, IllegalArgumentException.class);

                    log.error("Check arguments. " + errorMessage(iae));
                    log.info("Command [" + cmdName + "] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

                    return EXIT_CODE_INVALID_ARGUMENTS;
                }

                return EXIT_CODE_UNEXPECTED_ERROR;
            }

            return EXIT_CODE_OK;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return JMX_CMD_HND;
        }

        /** */
        private IgniteEx ignite(ConnectionAndSslParameters<IgniteDataTransferObject> p) {
            int port = p.port();

            if (port == this.port)
                return ignite;

            for (Ignite node : IgnitionEx.allGrids()) {
                Integer nodePort = ((IgniteEx)node).localNode().<Integer>attribute(CLIENT_LISTENER_PORT);

                if (nodePort != null && port == nodePort) {
                    this.port = port;
                    ignite = (IgniteEx)node;

                    return ignite;
                }
            }

            throw new IllegalStateException("Unknown grid for port: " + port);
        }

        /** {@inheritDoc} */
        @Override public <T> T getLastOperationResult() {
            return (T)res;
        }

        /** {@inheritDoc} */
        @Override public void flushLogger() {
            log.flush();
        }

        /** */
        private static String toString(Object val) {
            if (val == null || (isBoolean(val.getClass()) && !(boolean)val))
                return "";

            if (val.getClass().isArray()) {
                int length = Array.getLength(val);

                if (length == 0)
                    return "";

                if (val.getClass() == char[].class)
                    return new String((char[])val);

                StringJoiner sj = new StringJoiner(",");

                for (int i = 0; i < length; i++)
                    sj.add(toString(Array.get(val, i)));

                return sj.toString();
            }

            return Objects.toString(val);
        }
    }

    /** */
    protected int commandHandlerExtraLines() {
        return CLI_CMD_HND.equals(commandHandler)
            ? 11
            : 0;
    }
}
