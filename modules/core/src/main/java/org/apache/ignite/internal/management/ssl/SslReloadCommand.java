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

package org.apache.ignite.internal.management.ssl;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.jetbrains.annotations.Nullable;

/** */
public class SslReloadCommand implements ComputeCommand<SslReloadCommandArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Reload TLS certificates on all cluster nodes by re-reading the configured key and trust stores. " +
            "New connections use the updated certificates while established sessions are not interrupted";
    }

    /** {@inheritDoc} */
    @Override public Class<SslReloadCommandArg> argClass() {
        return SslReloadCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SslReloadTask> taskClass() {
        return SslReloadTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, SslReloadCommandArg arg) {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SslReloadCommandArg arg, String res, Consumer<String> printer) {
        printer.accept(res);
    }

    /** {@inheritDoc} */
    @Override public @Nullable String confirmationPrompt(SslReloadCommandArg arg) {
        // A dry run changes nothing, so there is nothing to confirm.
        return arg.dryRun()
            ? null
            : "Warning: the command will reload TLS certificates on all cluster nodes.";
    }
}
