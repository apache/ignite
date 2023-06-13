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

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.PreparableCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTask;

/** Destroy caches. */
public class CacheDestroyCommand
    implements ComputeCommand<CacheDestroyCommandArg, Void>, PreparableCommand<CacheDestroyCommandArg, Void> {
    /** Confirmation message format. */
    public static final String CONFIRM_MSG = "Warning! The command will destroy %d caches: %s.\n" +
        "If you continue, the cache data will be impossible to recover.";

    /** No user-created caches exists message. */
    public static final String NOOP_MSG = "No user-created caches exist.";

    /** Result message. */
    public static final String RESULT_MSG = "The following caches have been stopped: %s.";

    /** {@inheritDoc} */
    @Override public String description() {
        return "Permanently destroy specified caches";
    }

    /** {@inheritDoc} */
    @Override public boolean prepare(GridClient cli, CacheDestroyCommandArg arg, Consumer<String> printer) throws GridClientException {
        if (arg.destroyAllCaches()) {
            Set<String> caches = new TreeSet<>();

            for (GridClientNode node : cli.compute().nodes(GridClientNode::connectable))
                caches.addAll(node.caches().keySet());

            arg.caches(caches.toArray(U.EMPTY_STRS));
        }

        if (F.isEmpty(arg.caches())) {
            printer.accept(NOOP_MSG);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(CacheDestroyCommandArg arg) {
        return String.format(CONFIRM_MSG, arg.caches().length, S.joinToString(Arrays.asList(arg.caches()), ", ", "..", 80, 0));
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheDestroyCommandArg arg, Void res, Consumer<String> printer) {
        printer.accept(String.format(RESULT_MSG, F.concat(Arrays.asList(arg.caches()), ", ")));
    }

    /** {@inheritDoc} */
    @Override public Class<CacheDestroyCommandArg> argClass() {
        return CacheDestroyCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class taskClass() {
        return VisorCacheStopTask.class;
    }
}
