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
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/** Task of an {@code --ssl} command: every node answers on its own, and the command fails only once every node is in the report. */
public abstract class SslTask extends VisorMultiNodeTask<NoArg, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        boolean failed = false;

        for (ComputeJobResult jobRes : results) {
            IgniteException e = jobRes.getException();

            if (e == null)
                res.append(jobRes.getData().toString());
            else {
                failed = true;

                String id = jobRes.getNode().id().toString();
                String msg = e.getMessage() != null ? e.getMessage() : e.toString();

                res.append(msg.startsWith(id) ? msg : id + ": " + msg);
            }

            res.append('\n');
        }

        if (failed)
            throw new IgniteException(res.toString());

        return res.toString();
    }

    /**
     * @param ignite Node.
     * @return Components of the node whose certificates the commands reload and report, empty if the node uses no SSL.
     */
    static Collection<SslContextReloadable> reloadables(IgniteEx ignite) {
        return ignite.context().internalSubscriptionProcessor().sslContexts().reloadables();
    }
}
