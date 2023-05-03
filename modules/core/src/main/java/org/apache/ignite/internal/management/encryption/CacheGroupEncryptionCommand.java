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

package org.apache.ignite.internal.management.encryption;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** */
abstract class CacheGroupEncryptionCommand<T>
    implements Command<EncryptionCacheGroupArg, VisorCacheGroupEncryptionTaskResult<T>> {
    /** {@inheritDoc} */
    @Override public void printResult(
        EncryptionCacheGroupArg arg,
        VisorCacheGroupEncryptionTaskResult<T> res,
        Consumer<String> printer
    ) {
        Map<UUID, IgniteException> exceptions = res.exceptions();

        for (Map.Entry<UUID, IgniteException> entry : exceptions.entrySet()) {
            printer.accept(INDENT + "Node " + entry.getKey() + ":");

            printer.accept(String.format("%sfailed to execute command for the cache group \"%s\": %s.",
                DOUBLE_INDENT, arg.cacheGroupName(), entry.getValue().getMessage()));
        }

        Map<UUID, T> results = res.results();

        for (Map.Entry<UUID, T> entry : results.entrySet()) {
            printer.accept(INDENT + "Node " + entry.getKey() + ":");

            printNodeResult(entry.getValue(), arg.cacheGroupName(), printer);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T2<Boolean, Object>> nodes, EncryptionCacheGroupArg arg) {
        return nodes.keySet();
    }

    /**
     * @param res Response.
     * @param grpName Cache group name.
     * @param printer Printer.
     */
    protected abstract void printNodeResult(T res, String grpName, Consumer<String> printer);
}
