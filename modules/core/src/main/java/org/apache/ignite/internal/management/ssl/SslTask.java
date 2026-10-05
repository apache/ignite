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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.CommandWarningException;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task of an {@code --ssl} command: every node answers with lines that start with its id, and the command lists all
 * of them. A node that failed fails the command, and a node that answered with a warning makes it end with one, but
 * only once every node is in the report: nodes act on their own, so the operator has to see each of them.
 *
 * @param <A> Argument.
 */
public abstract class SslTask<A extends IgniteDataTransferObject> extends VisorMultiNodeTask<A, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @return What to report for a node that left the cluster while the command ran.
     */
    protected abstract String leftCluster();

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        boolean failed = false;

        boolean warned = false;

        for (ComputeJobResult jobRes : results) {
            IgniteException e = jobRes.getException();

            if (e == null)
                res.append(jobRes.getData().toString());
            else if (X.hasCause(e, ClusterTopologyException.class, ClusterTopologyCheckedException.class))
                res.append(jobRes.getNode().id()).append(": ").append(leftCluster());
            else {
                if (X.hasCause(e, CommandWarningException.class))
                    warned = true;
                else
                    failed = true;

                String msg = e.getMessage() != null ? e.getMessage() : e.toString();

                // The job reports every node with its id; anything else that failed has to be attributed too.
                res.append(msg.startsWith(jobRes.getNode().id().toString())
                    ? msg
                    : jobRes.getNode().id() + ": " + msg);
            }

            res.append('\n');
        }

        if (failed)
            throw new IgniteException(res.toString());

        if (warned)
            throw warning(res.toString());

        return res.toString();
    }

    /**
     * @param res Report to return.
     * @return Exception that makes the command end with warnings and print the report.
     */
    static IgniteException warning(String res) {
        return new IgniteException(res, new CommandWarningException(new IgniteException(res)));
    }

    /**
     * @param ignite Node.
     * @return Line to answer with when the node has no transport serving SSL, or {@code null} if it has one.
     */
    static @Nullable String nothingServed(IgniteEx ignite) {
        Collection<SslContextReloadable> comps =
            ignite.context().internalSubscriptionProcessor().getSslContextReloadables();

        if (comps.isEmpty())
            return ignite.localNode().id() + ": SSL is not configured";

        // Configured but serving nothing is a different answer from not configured at all: it means a transport did
        // not start, which the operator would otherwise have to find out some other way.
        if (serving(ignite).isEmpty())
            return ignite.localNode().id() + ": SSL is configured, but no transport is serving it";

        return null;
    }

    /**
     * @param ignite Node.
     * @return Components of the node that serve a transport, sorted by the transports they serve, so that the report
     *      of a node does not depend on the order they started in.
     */
    static List<SslContextReloadable> serving(IgniteEx ignite) {
        List<SslContextReloadable> res = new ArrayList<>();

        // A provider whose transport never started serves nothing, so it has nothing to report either.
        for (SslContextReloadable comp : ignite.context().internalSubscriptionProcessor().getSslContextReloadables()) {
            if (!comp.users().isEmpty())
                res.add(comp);
        }

        res.sort(Comparator.comparing(comp -> String.join(", ", comp.users())));

        return res;
    }
}
