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

package org.apache.ignite.internal.management;

import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.visor.misc.VisorClusterChangeTagTask;
import org.apache.ignite.internal.visor.misc.VisorClusterChangeTagTaskResult;
import org.jetbrains.annotations.Nullable;

/** */
public class ChangeTagCommand implements ComputeCommand<ChangeTagCommandArg, VisorClusterChangeTagTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Change cluster tag to new value";
    }

    /** {@inheritDoc} */
    @Override public Class<ChangeTagCommandArg> argClass() {
        return ChangeTagCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorClusterChangeTagTask> taskClass() {
        return VisorClusterChangeTagTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        ChangeTagCommandArg arg,
        VisorClusterChangeTagTaskResult res,
        Consumer<String> printer
    ) {
        if (res.success())
            printer.accept("Cluster tag updated successfully, old tag was: " + res.tag());
        else
            printer.accept("Error has occurred during tag update: " + res.errorMessage());
    }

    /** {@inheritDoc} */
    @Override public @Nullable String confirmationPrompt(GridClient cli, ChangeTagCommandArg arg) {
        return "Warning: the command will change cluster tag.";
    }
}
