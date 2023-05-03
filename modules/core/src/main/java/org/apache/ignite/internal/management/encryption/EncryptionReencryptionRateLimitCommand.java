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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionRateTask;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** */
public class EncryptionReencryptionRateLimitCommand
    implements Command<EncryptionReencryptionRateLimitCommandArg, VisorCacheGroupEncryptionTaskResult<Double>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "View/change re-encryption rate limit";
    }

    /** {@inheritDoc} */
    @Override public Class<EncryptionReencryptionRateLimitCommandArg> argClass() {
        return EncryptionReencryptionRateLimitCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorReencryptionRateTask> taskClass() {
        return VisorReencryptionRateTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(
        Collection<T3<UUID, Boolean, Object>> nodes,
        EncryptionReencryptionRateLimitCommandArg arg
    ) {
        return nodes.stream().map(T3::get1).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        EncryptionReencryptionRateLimitCommandArg arg,
        VisorCacheGroupEncryptionTaskResult<Double> res,
        Consumer<String> printer
    ) {
        Map<UUID, IgniteException> exceptions = res.exceptions();

        for (Map.Entry<UUID, IgniteException> entry : exceptions.entrySet()) {
            printer.accept(INDENT + "Node " + entry.getKey() + ":");
            printer.accept(DOUBLE_INDENT +
                "failed to get/set re-encryption rate limit: " + entry.getValue().getMessage());
        }

        Map<UUID, Double> results = res.results();
        boolean read = arg.newLimit() == null;

        for (Map.Entry<UUID, Double> entry : results.entrySet()) {
            printer.accept(INDENT + "Node " + entry.getKey() + ":");

            Double rateLimit = read ? entry.getValue() : arg.newLimit();

            if (rateLimit == 0)
                printer.accept(DOUBLE_INDENT + "re-encryption rate is not limited.");
            else {
                printer.accept(String.format("%sre-encryption rate %s limited to %.2f MB/s.",
                    DOUBLE_INDENT, (read ? "is" : "has been"), rateLimit));
            }
        }

        if (read)
            return;

        printer.accept("");
        printer.accept("Note: the changed value of the re-encryption rate limit is not persisted. " +
            "When the node is restarted, the value will be set from the configuration.");
        printer.accept("");
    }
}
