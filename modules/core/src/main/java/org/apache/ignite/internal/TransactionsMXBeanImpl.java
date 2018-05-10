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

package org.apache.ignite.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.mxbean.TransactionsMXBean;

/**
 * TransactionsMXBean implementation.
 */
public class TransactionsMXBeanImpl implements TransactionsMXBean {
    /** */
    private final GridKernalContextImpl ctx;

    /**
     * @param ctx Context.
     */
    TransactionsMXBeanImpl(GridKernalContextImpl ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public String getActiveTransactions(Long minDuration, Integer minSize, String prj, String consistentIds,
        String xid, String lbRegex, Integer limit, String order, boolean detailed, boolean kill) {
        try {
            IgniteCompute compute = ctx.cluster().get().compute();

            VisorTxProjection proj = null;

            if (prj != null) {
                if ("clients".equals(prj))
                    proj = VisorTxProjection.CLIENT;
                else if ("servers".equals(prj))
                    proj = VisorTxProjection.SERVER;
            }

            List<String> consIds = null;

            if (consistentIds != null)
                consIds = Arrays.stream(consistentIds.split(",")).collect(Collectors.toList());

            VisorTxSortOrder sortOrder = null;

            if (order != null) {
                if ("DURATION".equals(order))
                    sortOrder = VisorTxSortOrder.DURATION;
                else if ("SIZE".equals(order))
                    sortOrder = VisorTxSortOrder.SIZE;
            }

            VisorTxTaskArg arg = new VisorTxTaskArg(kill ? VisorTxOperation.KILL : VisorTxOperation.LIST,
                limit, minDuration == null ? null : minDuration * 1000, minSize, null, proj, consIds, xid, lbRegex, sortOrder);

            Map<ClusterNode, VisorTxTaskResult> res = compute.execute(new VisorTxTask(),
                new VisorTaskArgument<>(ctx.cluster().get().localNode().id(), arg, false));

            if (detailed) {
                StringWriter sw = new StringWriter();

                PrintWriter w = new PrintWriter(sw);

                for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
                    if (entry.getValue().getInfos().isEmpty())
                        continue;

                    ClusterNode key = entry.getKey();

                    w.println(key.toString());

                    for (VisorTxInfo info : entry.getValue().getInfos())
                        w.println("    Tx: [xid=" + info.getXid() +
                            ", label=" + info.getLabel() +
                            ", state=" + info.getState() +
                            ", duration=" + info.getDuration() / 1000 +
                            ", isolation=" + info.getIsolation() +
                            ", concurrency=" + info.getConcurrency() +
                            ", timeout=" + info.getTimeout() +
                            ", size=" + info.getSize() +
                            ", dhtNodes=" + F.transform(info.getPrimaryNodes(), new IgniteClosure<UUID, String>() {
                            @Override public String apply(UUID id) {
                                return U.id8(id);
                            }
                        }) +
                            ']');
                }

                w.flush();

                return sw.toString();
            }
            else {
                int cnt = 0;

                for (VisorTxTaskResult result : res.values())
                    cnt += result.getInfos().size();

                return Integer.toString(cnt);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public long getTxTimeoutOnPartitionMapExchange() {
        return ctx.config().getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override public void setTxTimeoutOnPartitionMapExchange(long timeout) {
        try {
            ctx.grid().context().cache().setTxTimeoutOnPartitionMapExchange(timeout);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }
}


