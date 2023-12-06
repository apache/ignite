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

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** */
@IgniteExperimental
public class WalPrintCommand implements ComputeCommand<WalDeleteCommandArg, WalTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print absolute paths of unused archived wal segments on each node";
    }

    /** {@inheritDoc} */
    @Override public Class<WalPrintCommandArg> argClass() {
        return WalPrintCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<WalTask> taskClass() {
        return WalTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(WalDeleteCommandArg arg, WalTaskResult taskRes, Consumer<String> printer) {
        printer.accept("Unused wal segments per node:");
        printer.accept("");

        Map<String, Collection<String>> res = taskRes.results();
        Map<String, Exception> failRes = taskRes.exceptions();
        Map<String, ClusterNode> nodesInfo = taskRes.getNodesInfo();

        for (Map.Entry<String, Collection<String>> entry : res.entrySet()) {
            ClusterNode node = nodesInfo.get(entry.getKey());

            printer.accept("Node=" + node.getConsistentId());
            printer.accept(DOUBLE_INDENT + "addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()));

            for (String fileName : entry.getValue())
                printer.accept(INDENT + fileName);

            printer.accept("");
        }

        for (Map.Entry<String, Exception> entry : failRes.entrySet()) {
            ClusterNode node = nodesInfo.get(entry.getKey());

            printer.accept("Node=" + node.getConsistentId());
            printer.accept(DOUBLE_INDENT + "addresses " + U.addressesAsString(node.getAddresses(), node.getHostNames()));
            printer.accept(INDENT + "failed with error: " + entry.getValue().getMessage());
            printer.accept("");
        }
    }

    /** */
    public static class WalPrintCommandArg extends WalDeleteCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }
}
