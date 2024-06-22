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
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.tx.TxCommandArg;
import org.apache.ignite.internal.management.tx.TxInfo;
import org.apache.ignite.internal.management.tx.TxSortOrder;
import org.apache.ignite.internal.management.tx.TxTask;
import org.apache.ignite.internal.management.tx.TxTaskResult;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.mxbean.TransactionsMXBean;

/**
 * TransactionsMXBean implementation.
 */
public class TransactionsMXBeanImpl implements TransactionsMXBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public TransactionsMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public String getActiveTransactions(Long minDuration, Integer minSize, String prj, String consistentIds,
        String xid, String lbRegex, Integer limit, String order, boolean detailed, boolean kill) {
        try {
            String[] consIds = null;

            if (consistentIds != null)
                consIds = consistentIds.split(",");

            TxSortOrder sortOrder = null;

            if (order != null)
                sortOrder = TxSortOrder.valueOf(order.toUpperCase());

            TxCommandArg arg = new TxCommandArg();

            if (kill)
                arg.kill(true);

            arg.limit(limit);
            arg.minDuration(minDuration);
            arg.minSize(minSize);

            if (prj != null) {
                if ("clients".equals(prj))
                    arg.clients(true);
                else if ("servers".equals(prj))
                    arg.servers(true);
            }

            arg.nodes(consIds);
            arg.xid(xid);
            arg.label(lbRegex);
            arg.order(sortOrder);

            Map<ClusterNode, TxTaskResult> res = ctx.task().execute(
                new TxTask(),
                new VisorTaskArgument<>(ctx.cluster().get().localNode().id(), arg, false)
            ).get().result();

            if (detailed) {
                StringWriter sw = new StringWriter();

                PrintWriter w = new PrintWriter(sw);

                for (Map.Entry<ClusterNode, TxTaskResult> entry : res.entrySet()) {
                    if (entry.getValue().getInfos().isEmpty())
                        continue;

                    ClusterNode key = entry.getKey();

                    w.println(key.toString());

                    for (TxInfo info : entry.getValue().getInfos())
                        w.println(info.toUserString());
                }

                w.flush();

                return sw.toString();
            }
            else {
                int cnt = 0;

                for (TxTaskResult result : res.values())
                    cnt += result.getInfos().size();

                return Integer.toString(cnt);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(String xid) {
        A.notNull(xid, "xid");

        try {
            TxCommandArg arg = new TxCommandArg();

            arg.kill(true);
            arg.limit(1);
            arg.xid(xid);

            ctx.task().execute(
                new TxTask(),
                new VisorTaskArgument<>(ctx.localNodeId(), arg, false)
            ).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
    @Override public void setTxKeyCollisionsInterval(int timeout) {
        ctx.cache().context().tm().collisionsDumpIntervalDistributed(timeout);
    }

    /** {@inheritDoc} */
    @Override public int getTxKeyCollisionsInterval() {
        return ctx.cache().context().tm().collisionsDumpInterval();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }
}


