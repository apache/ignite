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

package org.apache.ignite.internal.management.cache;

import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.processors.cache.ClearCachesTask;
import org.apache.ignite.internal.processors.cache.ClearCachesTaskResult;
import org.apache.ignite.internal.util.typedef.F;

/** Clear caches. */
public class CacheClearCommand implements ComputeCommand<CacheClearCommandArg, ClearCachesTaskResult> {
    /** Message that contains cleared caches. */
    public static final String CLEAR_MSG = "The following caches have been cleared: %s";

    /** Message that contains not-cleared caches (they don't exist). */
    public static final String SKIP_CLEAR_MSG = "The following caches don't exist: %s";

    /** Confirmation message format. */
    public static final String CONFIRM_MSG = "Warning! The command will clear all data from %d caches: %s.\n" +
        "If you continue, it will be impossible to recover cleared data.";

    /** {@inheritDoc} */
    @Override public String description() {
        return "Clear specified caches";
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(GridClient cli, CacheClearCommandArg arg) throws Exception {
        return String.format(CONFIRM_MSG, arg.caches().length, String.join(", ", arg.caches()));
    }

    /** {@inheritDoc} */
    @Override public Class<CacheClearCommandArg> argClass() {
        return CacheClearCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ClearCachesTask> taskClass() {
        return ClearCachesTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheClearCommandArg arg, ClearCachesTaskResult res, Consumer<String> printer) {
        if (!F.isEmpty(res.clearedCaches()))
            printer.accept(String.format(CLEAR_MSG, String.join(", ", res.clearedCaches())));

        if (!F.isEmpty(res.nonExistentCaches()))
            printer.accept(String.format(SKIP_CLEAR_MSG, String.join(", ", res.nonExistentCaches())));
    }
}
