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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.commandline.ArgumentParser;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.ConnectionAndSslParameters;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.jmx.CommandMBean;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.internal.management.jmx.CommandMBean.METHOD;

/**
 *
 */
public class GridCommandHandlerFactoryAbstractTest extends GridCommonAbstractTest {
    /** */
    public static final String JMX_INVOKER = "jmx";

    /** */
    public static final String CLI_INVOKER = "cli";

    /** */
    public String invoker = CLI_INVOKER;

    /** Command executor factory. */
    protected Function<IgniteLogger, CliFrontend> cmdHndFactory0 = log -> {
        switch (invoker) {
            case CLI_INVOKER:
                return new CliCmdFrontend(log);

            case JMX_INVOKER:
                return new JmxCmdFrontend(log);

            default:
                throw new IllegalArgumentException("Unknown invoker: " + invoker);
        }
    };

    /** Command executor factory. */
    protected Supplier<CliFrontend> cmdHndFactory = () -> cmdHndFactory0.apply(null);

    /** */
    public static interface CliFrontend extends ToIntFunction<List<String>> {
        /** */
        public <T> T result();

        /** */
        public void flushLogger();
    }

    /** */
    private static class CliCmdFrontend implements CliFrontend {
        /** */
        private final CommandHandler hnd;

        /** */
        public CliCmdFrontend() {
            this.hnd = new CommandHandler();
        }

        /** */
        public CliCmdFrontend(@Nullable IgniteLogger log) {
            this.hnd = log == null ? new CommandHandler() : new CommandHandler(log);
        }

        /** {@inheritDoc} */
        @Override public int applyAsInt(List<String> rawArgs) {
            return hnd.execute(rawArgs);
        }

        /** {@inheritDoc} */
        @Override public <T> T result() {
            return hnd.result();
        }

        /** {@inheritDoc} */
        @Override public void flushLogger() {
            // Flush all Logger handlers to make log data available to test.
            U.<IgniteLoggerEx>field(hnd, "logger").flush();
        }
    }

    /** */
    private static class JmxCmdFrontend implements CliFrontend {
        /** */
        private IgniteLoggerEx log;

        /** */
        private IgniteEx grid;

        /** */
        private Object res;

        /** */
        public JmxCmdFrontend(@Nullable IgniteLogger log) {
            this.log = (IgniteLoggerEx)log;
        }

        /** {@inheritDoc} */
        @Override public int applyAsInt(List<String> value) {
            if (grid == null) {
                grid = IgnitionEx.localIgnite();

                if (log == null)
                    log = (IgniteLoggerEx)grid.log();
            }

            ArgumentParser parser = new ArgumentParser(log, new IgniteCommandRegistry());

            ConnectionAndSslParameters<IgniteDataTransferObject> p = parser.parseAndValidate(value);

            List<String> grp = p.root() == null ? null : singletonList(toFormattedCommandName(p.root().getClass()));

            DynamicMBean mbean = getMxBean(
                grid.context().igniteInstanceName(),
                "management",
                toFormattedCommandName(p.command().getClass()),
                CommandMBean.class
            );

            List<String> params = new ArrayList<>();
            List<String> signature = new ArrayList<>();

            Consumer<Field> fldCnsmr = fld -> {
                signature.add(String.class.getName());

                Object val = U.field(p.commandArg(), fld.getName());

                params.add(val == null ? "" : toString(val.getClass(), val));
            };

            visitCommandParams(p.command().argClass(), fldCnsmr, fldCnsmr, (optional, flds) -> flds.forEach(fldCnsmr));

            try {
                res = mbean.invoke(METHOD, params.toArray(X.EMPTY_OBJECT_ARRAY), signature.toArray(U.EMPTY_STRS));
            }
            catch (MBeanException | ReflectionException e) {
                throw new IgniteException(e);
            }

            return EXIT_CODE_OK;
        }

        /** {@inheritDoc} */
        @Override public <T> T result() {
            return (T)res;
        }

        /** {@inheritDoc} */
        @Override public void flushLogger() {
            log.flush();
        }

        /** */
        private static <T> String toString(Class<?> cls, Object val) {
            return S.toString((Class<T>)cls, (T)val);
        }
    }
}
