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

package org.apache.ignite.platform;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobFailoverException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to test exception mappings.
 */
@SuppressWarnings("unused")  // Used by .NET ExceptionsTest.
public class PlatformExceptionTask extends ComputeTaskAdapter<String, String> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String arg) {
        assert arg != null;

        switch (arg) {
            case "IllegalArgumentException": throw new IllegalArgumentException(arg);
            case "IllegalStateException": throw new IllegalStateException(arg);
            case "UnsupportedOperationException": throw new UnsupportedOperationException(arg);
            case "IgniteException": throw new IgniteException(arg);
            case "BinaryObjectException": throw new BinaryObjectException(arg);
            case "ClusterGroupEmptyException": throw new ClusterGroupEmptyException(arg);
            case "ClusterTopologyException": throw new ClusterTopologyException(arg);
            case "ComputeExecutionRejectedException": throw new ComputeExecutionRejectedException(arg);
            case "ComputeJobFailoverException": throw new ComputeJobFailoverException(arg);
            case "ComputeTaskCancelledException": throw new ComputeTaskCancelledException(arg);
            case "ComputeTaskTimeoutException": throw new ComputeTaskTimeoutException(arg);
            case "ComputeUserUndeclaredException": throw new ComputeUserUndeclaredException(arg);
            case "CacheException": throw new CacheException(arg);
            case "CacheLoaderException": throw new CacheLoaderException(arg);
            case "CacheWriterException": throw new CacheWriterException(arg);
            case "EntryProcessorException": throw new EntryProcessorException(arg);
            case "TransactionOptimisticException": throw new TransactionOptimisticException(arg);
            case "TransactionTimeoutException": throw new TransactionTimeoutException(arg);
            case "TransactionRollbackException": throw new TransactionRollbackException(arg);
            case "TransactionHeuristicException": throw new TransactionHeuristicException(arg);
            case "TransactionDeadlockException": throw new TransactionDeadlockException(arg);
            case "IgniteFutureCancelledException": throw new IgniteFutureCancelledException(arg);
            case "ServiceDeploymentException": throw new ServiceDeploymentException(arg,
                    Collections.singletonList(new ServiceConfiguration().setName("foo")));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }
}
