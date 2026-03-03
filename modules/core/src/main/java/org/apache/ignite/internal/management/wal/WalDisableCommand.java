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

package org.apache.ignite.internal.management.wal;

import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;

/** */
public class WalDisableCommand implements ComputeCommand<WalDisableCommand.WalDisableCommandArg, WalSetStateTaskResult> {
    /** {@inheritDoc} */
    @Override public Class<WalSetStateTask> taskClass() {
        return WalSetStateTask.class;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Disable WAL for specific cache groups";
    }

    /** {@inheritDoc} */
    @Override public Class<WalDisableCommandArg> argClass() {
        return WalDisableCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(WalDisableCommandArg arg) {
        return "Are you sure? Any node failure without WAL can lead to the loss of all PDS data. CDC events will be lost without WAL.";
    }

    /** {@inheritDoc} */
    @Override public void printResult(WalDisableCommandArg arg, WalSetStateTaskResult res, Consumer<String> printer) {
        res.print(false, printer);
    }

    /** */
    public static class WalDisableCommandArg extends WalStateCommandArg {
        /** */
        private static final long serialVersionUID = 0;

        // No-op.
    }
}
