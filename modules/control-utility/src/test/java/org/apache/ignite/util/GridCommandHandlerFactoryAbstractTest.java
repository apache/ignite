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

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCommandHandlerFactoryAbstractTest extends GridCommonAbstractTest {
    /** Command executor factory. */
    protected Supplier<CliFrontend> cmdHndFactory = CommandHandlerCliFrontend::new;

    /** Command executor factory. */
    protected Function<IgniteLogger, CliFrontend> cmdHndFactory0 = CommandHandlerCliFrontend::new;

    /** */
    public static interface CliFrontend extends ToIntFunction<List<String>> {
        /** */
        public <T> T result();

        /** */
        public void flushLogger();
    }

    /** */
    private class CommandHandlerCliFrontend implements CliFrontend {
        /** */
        private final CommandHandler hnd;

        /** */
        public CommandHandlerCliFrontend() {
            this.hnd = new CommandHandler();
        }

        /** */
        public CommandHandlerCliFrontend(IgniteLogger log) {
            this.hnd = new CommandHandler(log);
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

        /** {@inheritDoc} */
        @Override public int applyAsInt(List<String> rawArgs) {
            return hnd.execute(rawArgs);
        }
    }
}
