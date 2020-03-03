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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
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
    public TransactionsMXBeanImpl(GridKernalContextImpl ctx) {
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

            if (order != null)
                sortOrder = VisorTxSortOrder.valueOf(order.toUpperCase());

            VisorTxTaskArg arg = new VisorTxTaskArg(kill ? VisorTxOperation.KILL : VisorTxOperation.LIST,
                limit, minDuration == null ? null : minDuration * 1000, minSize, null, proj, consIds, xid, lbRegex, sortOrder, null);

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
                        w.println(info.toUserString());
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
            ctx.cache().context().tm().setTxTimeoutOnPartitionMapExchange(timeout);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean getTxOwnerDumpRequestsAllowed() {
        return ctx.cache().context().tm().txOwnerDumpRequestsAllowed();
    }

    /** {@inheritDoc} */
    @Override public void setTxOwnerDumpRequestsAllowed(boolean allowed) {
        ctx.cache().context().tm().setTxOwnerDumpRequestsAllowedDistributed(allowed);
    }

    /** {@inheritDoc} */
    @Override public long getLongTransactionTimeDumpThreshold() {
        return ctx.cache().context().tm().longTransactionTimeDumpThreshold();
    }

    /** {@inheritDoc} */
    @Override public void setLongTransactionTimeDumpThreshold(long threshold) {
        ctx.cache().context().tm().longTransactionTimeDumpThresholdDistributed(threshold);
    }

    /** {@inheritDoc} */
    @Override public double getTransactionTimeDumpSamplesCoefficient() {
        return ctx.cache().context().tm().transactionTimeDumpSamplesCoefficient();
    }

    /** {@inheritDoc} */
    @Override public void setTransactionTimeDumpSamplesCoefficient(double coefficient) {
        ctx.cache().context().tm().transactionTimeDumpSamplesCoefficientDistributed(coefficient);
    }

    /** {@inheritDoc} */
    @Override public int getTransactionTimeDumpSamplesPerSecondLimit() {
        return ctx.cache().context().tm().transactionTimeDumpSamplesPerSecondLimit();
    }

    /** {@inheritDoc} */
    @Override public void setTransactionTimeDumpSamplesPerSecondLimit(int limit) {
        ctx.cache().context().tm().longTransactionTimeDumpSamplesPerSecondLimit(limit);
    }

    /** {@inheritDoc} */
    @Override public void setLongOperationsDumpTimeout(long timeout) {
        ctx.cache().context().tm().longOperationsDumpTimeoutDistributed(timeout);
    }

    /** {@inheritDoc} */
    @Override public long getLongOperationsDumpTimeout() {
        return ctx.cache().context().tm().longOperationsDumpTimeout();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }
}


