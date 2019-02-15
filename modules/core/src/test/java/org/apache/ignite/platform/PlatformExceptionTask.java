/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.platform;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.Nullable;

import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Task to test exception mappings.
 */
@SuppressWarnings("unused")  // Used by .NET ExceptionsTest.
public class PlatformExceptionTask extends ComputeTaskAdapter<String, String> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
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
