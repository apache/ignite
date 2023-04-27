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

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskResult;
import org.apache.ignite.internal.visor.encryption.VisorEncryptionKeyIdsTask;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;

/** */
public class EncryptionCacheKeyIdsCommand extends CacheGroupEncryptionCommand<List<Integer>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "View encryption key identifiers of the cache group";
    }

    /** {@inheritDoc} */
    @Override public Class<EncryptionCacheGroupArg> argClass() {
        return EncryptionCacheGroupArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorEncryptionKeyIdsTask> taskClass() {
        return VisorEncryptionKeyIdsTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        EncryptionCacheGroupArg arg,
        VisorCacheGroupEncryptionTaskResult<List<Integer>> res,
        Consumer<String> printer
    ) {
        printer.accept("Encryption key identifiers for cache: " + arg.cacheGroupName());

        super.printResult(arg, res, printer);
    }

    /** {@inheritDoc} */
    @Override protected void printNodeResult(List<Integer> res, String grpName, Consumer<String> printer) {
        if (F.isEmpty(res)) {
            printer.accept(DOUBLE_INDENT + "---");

            return;
        }

        for (int i = 0; i < res.size(); i++)
            printer.accept(DOUBLE_INDENT + res.get(i) + (i == 0 ? " (active)" : ""));
    }
}
