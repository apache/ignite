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

package org.apache.ignite.compute;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;

/**
 * Result of remote job which gets passed into {@link ComputeTask#result(ComputeJobResult, List)}
 * method.
 */
public interface ComputeJobResult {
    /**
     * Gets job context. Use job context to access job unique ID or to get/set
     * jobs attributes. Context is attached to a job and travels with it wherever
     * it goes. For example, if a job gets failed-over from one node to another,
     * then its context will be failed over with it and all attributes that
     * were set on the job on the first node will be available on the new node.
     *
     * @return Job context.
     */
    public ComputeJobContext getJobContext();

    /**
     * Gets data returned by remote job if it didn't fail. This data is the
     * object returned from {@link ComputeJob#execute()} method.
     * <p>
     * Note that if task is annotated with {@link ComputeTaskNoResultCache} annotation,
     * then job results will not be cached and will be available only in
     * {@link ComputeTask#result(ComputeJobResult, List)} method for every individual job,
     * but not in {@link ComputeTask#reduce(List)} method. This feature was added to
     * avoid excessive storing of overly large results.
     *
     * @param <T> Type of the return value returning from {@link ComputeJob#execute()} method.
     * @return Data returned by remote job's {@link ComputeJob#execute()} method if it didn't fail.
     */
    public <T> T getData();

    /**
     * Gets exception produced by execution of remote job, or {@code null} if
     * remote execution finished normally and did not produce any exceptions.
     *
     * @return {@link IgniteException} produced by execution of remote job or {@code null} if
     *      no exception was produced.
     *      <p>
     *      Note that if remote job resulted in {@link RuntimeException}
     *      or {@link Error} then they will be wrapped into {@link ComputeUserUndeclaredException}
     *      returned by this method.
     *      <p>
     *      If job on remote node was rejected (cancelled while it was on waiting queue), then
     *      {@link ComputeExecutionRejectedException} will be returned.
     *      <p>
     *      If node on which job was computing failed, then {@link ClusterTopologyException} is
     *      returned.
     */
    public IgniteException getException();

    /**
     * Gets local instance of remote job returned by {@link ComputeTask#map(List, Object)} method.
     *
     * @param <T> Type of {@link ComputeJob} that was sent to remote node.
     * @return Local instance of remote job returned by {@link ComputeTask#map(List, Object)} method.
     */
    public <T extends ComputeJob> T getJob();

    /**
     * Gets node this job executed on.
     *
     * @return Node this job executed on.
     */
    public ClusterNode getNode();

    /**
     * Gets job cancellation status. Returns {@code true} if job received cancellation
     * request on remote node. Note that job, after receiving cancellation request, will still
     * need to finish and return, hence {@link #getData()} method may contain
     * execution result even if the job was canceled.
     * <p>
     * Job can receive cancellation request if the task was explicitly cancelled
     * from future (see {@link ComputeTaskFuture#cancel()}) or if task completed prior
     * to getting results from all remote jobs.
     *
     * @return {@code true} if job received cancellation request and {@code false} otherwise.
     */
    public boolean isCancelled();
}