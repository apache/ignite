/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;

/**
 * Array-based query processor.
 */
public class ArrayCalciteQueryProcessor extends AbstractCalciteQueryProcessor<Object[]> {
    /**
     * @param ctx Kernal context.
     */
    public ArrayCalciteQueryProcessor(GridKernalContext ctx) {
        super(
            ctx,
            ctx.failure(),
            new SchemaHolderImpl<>(ctx),
            new QueryPlanCacheImpl(ctx),
            new MailboxRegistryImpl<>(ctx),
            new QueryTaskExecutorImpl(ctx),
            new ExecutionServiceImpl<>(ctx),
            new PartitionServiceImpl(ctx),
            new MessageServiceImpl(ctx),
            new MappingServiceImpl(ctx),
            new ExchangeServiceImpl<>(ctx),
            new ArrayRowEngineFactory());
    }
}
