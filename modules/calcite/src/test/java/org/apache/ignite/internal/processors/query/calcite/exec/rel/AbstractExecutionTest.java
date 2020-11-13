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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 *
 */
public class AbstractExecutionTest extends GridCommonAbstractTest {
    /** */
    private Throwable lastE;

    /** */
    private Map<UUID, QueryTaskExecutorImpl> taskExecutors;

    /** */
    private Map<UUID, ExchangeServiceImpl> exchangeServices;

    /** */
    private Map<UUID, MailboxRegistryImpl> mailboxRegistries;

    /** */
    private List<UUID> nodes;

    /** */
    protected int nodesCnt = 3;

    /** */
    @Before
    public void setup() throws Exception {
        nodes = IntStream.range(0, nodesCnt)
            .mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());

        taskExecutors = new HashMap<>(nodes.size());
        exchangeServices = new HashMap<>(nodes.size());
        mailboxRegistries = new HashMap<>(nodes.size());

        TestIoManager mgr = new TestIoManager();

        for (UUID uuid : nodes) {
            GridTestKernalContext kernal = newContext();

            QueryTaskExecutorImpl taskExecutor = new QueryTaskExecutorImpl(kernal);
            taskExecutor.stripedThreadPoolExecutor(new IgniteStripedThreadPoolExecutor(
                kernal.config().getQueryThreadPoolSize(),
                kernal.igniteInstanceName(),
                "calciteQry",
                this::handle,
                true,
                DFLT_THREAD_KEEP_ALIVE_TIME
            ));
            taskExecutors.put(uuid, taskExecutor);

            MailboxRegistryImpl mailboxRegistry = new MailboxRegistryImpl(kernal);

            mailboxRegistries.put(uuid, mailboxRegistry);

            MessageServiceImpl msgSvc = new TestMessageServiceImpl(kernal, mgr);
            msgSvc.localNodeId(uuid);
            msgSvc.taskExecutor(taskExecutor);
            mgr.register(msgSvc);

            ExchangeServiceImpl exchangeSvc = new ExchangeServiceImpl(kernal);
            exchangeSvc.taskExecutor(taskExecutor);
            exchangeSvc.messageService(msgSvc);
            exchangeSvc.mailboxRegistry(mailboxRegistry);
            exchangeSvc.init();

            exchangeServices.put(uuid, exchangeSvc);
        }
    }

    /** */
    @After
    public void tearDown() {
        taskExecutors.values().forEach(QueryTaskExecutorImpl::tearDown);

        if (lastE != null)
            throw new AssertionError(lastE);
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected List<UUID> nodes() {
        return nodes;
    }

    /** */
    protected ExchangeService exchangeService(UUID nodeId) {
        return exchangeServices.get(nodeId);
    }

    /** */
    protected MailboxRegistry mailboxRegistry(UUID nodeId) {
        return mailboxRegistries.get(nodeId);
    }

    /** */
    protected QueryTaskExecutor taskExecutor(UUID nodeId) {
        return taskExecutors.get(nodeId);
    }

    /** */
    protected ExecutionContext<Object[]> executionContext(UUID nodeId, UUID qryId, long fragmentId) {
        FragmentDescription fragmentDesc = new FragmentDescription(fragmentId, null, null, null);
        return new ExecutionContext<>(
            taskExecutor(nodeId),
            PlanningContext.builder()
                .localNodeId(nodeId)
                .logger(log())
                .build(),
            qryId, fragmentDesc, ArrayRowHandler.INSTANCE, ImmutableMap.of());
    }

    /** */
    private void handle(Thread t, Throwable ex) {
        log().error(ex.getMessage(), ex);
        lastE = ex;
    }

    /** */
    private static class TestMessageServiceImpl extends MessageServiceImpl {
        /** */
        private final TestIoManager mgr;

        /** */
        private TestMessageServiceImpl(GridKernalContext kernal, TestIoManager mgr) {
            super(kernal);
            this.mgr = mgr;
        }

        /** {@inheritDoc} */
        @Override public void send(UUID nodeId, CalciteMessage msg) {
            mgr.send(localNodeId(), nodeId, msg);
        }

        /** {@inheritDoc} */
        @Override public boolean alive(UUID nodeId) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected void prepareMarshal(Message msg) {
            // No-op;
        }

        /** {@inheritDoc} */
        @Override protected void prepareUnmarshal(Message msg) {
            // No-op;
        }
    }
}
