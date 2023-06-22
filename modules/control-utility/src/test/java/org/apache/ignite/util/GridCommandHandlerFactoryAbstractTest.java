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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
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

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.setupJavaLogger;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.management.api.CommandUtils.cmdKey;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.internal.managers.management.CommandMBean.INVOKE;
import static org.apache.ignite.internal.managers.management.CommandMBean.LAST_RES_METHOD;

/** Class to check command execution via all available handlers. */
@RunWith(Parameterized.class)
public class GridCommandHandlerFactoryAbstractTest extends GridCommonAbstractTest {
    /** @see JmxCommandHandler */
    public static final String JMX_CMD_HND = "jmx";

    /** @see CliCommandHandler */
    public static final String CLI_CMD_HND = "cli";

    /** */
    public static final List<String> CMD_HNDS = Arrays.asList(JMX_CMD_HND, CLI_CMD_HND);

    /** */
    @Parameterized.Parameter
    public String commandHandler;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return CMD_HNDS;
    }

    /** */
    protected TestCommandHandler newCommandHandler() {
        return newCommandHandler(null);
    }

    /** Command executor factory. */
    protected TestCommandHandler newCommandHandler(@Nullable IgniteLogger log) {
        switch (commandHandler) {
            case CLI_CMD_HND:
                return new CliCommandHandler(log);

            case JMX_CMD_HND:
                return new JmxCommandHandler(log);

            default:
                throw new IllegalArgumentException("Unknown handler: " + commandHandler);
        }
    }

    /** */
    public interface TestCommandHandler {
        /** */
        public <T> T getLastOperationResult();

        /** */
        public void flushLogger();

        /** */
        public int execute(List<String> rawArgs);
    }

    /** */
    private static class CliCommandHandler implements TestCommandHandler {
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
    }

    /** */
    private static class JmxCommandHandler implements TestCommandHandler {
        /** */
        private int port;

        /** */
        private IgniteEx ignite;

        /** */
        private IgniteLoggerEx log;

        /** */
        private Object res;

        /** */
        public JmxCommandHandler(@Nullable IgniteLogger log) {
            this.log = (IgniteLoggerEx)(log == null ? setupJavaLogger("jmx-invoker", CommandHandler.class) : log);
        }

        /** {@inheritDoc} */
        @Override public int execute(List<String> value) {
            String commandName = null;

            try {
                ArgumentParser parser = new ArgumentParser(log, new IgniteCommandRegistry());

                ConnectionAndSslParameters<IgniteDataTransferObject> p = parser.parseAndValidate(value);

                commandName = toFormattedCommandName(p.cmdPath().peekLast().getClass()).toUpperCase();

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

                visitCommandParams(p.command().argClass(), fldCnsmr, fldCnsmr, (optional, flds) -> flds.forEach(fldCnsmr));

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
                    log.info("Command [" + commandName + "] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

                    return EXIT_CODE_INVALID_ARGUMENTS;
                }

                return EXIT_CODE_UNEXPECTED_ERROR;
            }

            return EXIT_CODE_OK;
        }

        /** */
        private IgniteEx ignite(ConnectionAndSslParameters<IgniteDataTransferObject> p) {
            int port = p.port();

            if (port == this.port)
                return ignite;

            for (Ignite node : IgnitionEx.allGrids()) {
                Integer nodePort = ((IgniteEx)node).localNode().<Integer>attribute(ATTR_REST_TCP_PORT);

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

                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < length; i++) {
                    if (i != 0)
                        sb.append(',');

                    sb.append(toString(Array.get(val, i)));
                }

                return sb.toString();
            }

            return Objects.toString(val);
        }
    }

    /** */
    protected int commandHandlerExtraLines() {
        return commandHandler.equals(CLI_CMD_HND) ? 11 : 0;
    }
}
