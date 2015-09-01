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

package org.apache.ignite.spi.collision.priorityqueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;

/**
 * This class provides implementation for Collision SPI based on priority queue. Jobs are first ordered
 * by their priority, if one is specified, and only first {@link #getParallelJobsNumber()} jobs
 * is allowed to execute in parallel. Other jobs will be queued up.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 * <li>
 *      Number of jobs that can be executed in parallel (see {@link #setParallelJobsNumber(int)}).
 *      This number should usually be set to no greater than number of threads in the execution thread pool.
 * </li>
 * <li>
 *      Priority attribute session key (see {@link #getPriorityAttributeKey()}). Prior to
 *      returning from {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method, task implementation should
 *      set a value into the task session keyed by this attribute key. See {@link org.apache.ignite.compute.ComputeTaskSession}
 *      for more information about task session.
 * </li>
 * <li>
 *      Priority attribute job context key (see {@link #getJobPriorityAttributeKey()}).
 *      It is used for specifying job priority.
 *      See {@link org.apache.ignite.compute.ComputeJobContext} for more information about job context.
 * </li>
 * <li>Default priority value (see {@link #getDefaultPriority()}). It is used when no priority is set.</li>
 * <li>
 *      Default priority increase value (see {@link #getStarvationIncrement()}).
 *      It is used for increasing priority when job gets bumped down.
 *      This future is used for preventing starvation waiting jobs execution.
 * </li>
 * <li>
 *      Default increasing priority flag value (see {@link #isStarvationPreventionEnabled()}).
 *      It is used for enabling increasing priority when job gets bumped down.
 *      This future is used for preventing starvation waiting jobs execution.
 * </li>
 * </ul>
 * Below is a Java example of configuration for priority collision SPI:
 * <pre name="code" class="java">
 * PriorityQueueCollisionSpi colSpi = new PriorityQueueCollisionSpi();
 *
 * // Execute all jobs sequentially by setting parallel job number to 1.
 * colSpi.setParallelJobsNumber(1);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default collision SPI.
 * cfg.setCollisionSpi(colSpi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is Spring XML configuration example:
 * <pre name="code" class="xml">
 * &lt;property name="collisionSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi"&gt;
 *         &lt;property name="priorityAttributeKey" value="myPriorityAttributeKey"/&gt;
 *         &lt;property name="parallelJobsNumber" value="10"/&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <h1 class="header">Coding Example</h1>
 * Here is an example of a grid tasks that uses priority collision SPI configured in example above.
 * Note that priority collision resolution is absolutely transparent to the user and is simply a matter of proper
 * grid configuration. Also, priority may be defined only for task (it can be defined within the task,
 * not at a job level). All split jobs will be started with priority declared in their owner task.
 * <p>
 * This example demonstrates how urgent task may be declared with a higher priority value.
 * Priority SPI guarantees (see its configuration in example above, where number of parallel
 * jobs is set to {@code 1}) that all jobs from {@code MyGridUrgentTask} will most likely
 * be activated first (one by one) and jobs from {@code MyGridUsualTask} with lowest priority
 * will wait. Once higher priority jobs complete, lower priority jobs will be scheduled.
 * <pre name="code" class="java">
 * public class MyGridUsualTask extends ComputeTaskSplitAdapter&lt;Object, Object&gt; {
 *    public static final int SPLIT_COUNT = 20;
 *
 *    &#64;TaskSessionResource
 *    private ComputeTaskSession taskSes;
 *
 *    &#64;Override
 *    protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, Object arg) throws IgniteCheckedException {
 *        ...
 *        // Set low task priority (note that attribute name is used by the SPI
 *        // and should not be changed).
 *        taskSes.setAttribute("grid.task.priority", 5);
 *
 *        Collection&lt;ComputeJob&gt; jobs = new ArrayList&lt;ComputeJob&gt;(SPLIT_COUNT);
 *
 *        for (int i = 1; i &lt;= SPLIT_COUNT; i++) {
 *            jobs.add(new ComputeJobAdapter&lt;Integer&gt;(i) {
 *                ...
 *            });
 *        }
 *        ...
 *    }
 * }
 * </pre>
 * and
 * <pre name="code" class="java">
 * public class MyGridUrgentTask extends ComputeTaskSplitAdapter&lt;Object, Object&gt; {
 *    public static final int SPLIT_COUNT = 5;
 *
 *    &#64;TaskSessionResource
 *    private ComputeTaskSession taskSes;
 *
 *    &#64;Override
 *    protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, Object arg) throws IgniteCheckedException {
 *        ...
 *        // Set high task priority (note that attribute name is used by the SPI
 *        // and should not be changed).
 *        taskSes.setAttribute("grid.task.priority", 10);
 *
 *        Collection&lt;ComputeJob&gt; jobs = new ArrayList&lt;ComputeJob&gt;(SPLIT_COUNT);
 *
 *        for (int i = 1; i &lt;= SPLIT_COUNT; i++) {
 *            jobs.add(new ComputeJobAdapter&lt;Integer&gt;(i) {
 *                ...
 *            });
 *        }
 *        ...
 *    }
 * }
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = true)
public class PriorityQueueCollisionSpi extends IgniteSpiAdapter implements CollisionSpi,
    PriorityQueueCollisionSpiMBean {
    /**
     * Default number of parallel jobs allowed (set to number of cores times 2).
     */
    public static final int DFLT_PARALLEL_JOBS_NUM = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Default waiting jobs number. If number of waiting jobs exceed this number,
     * jobs will be rejected. Default value is {@link Integer#MAX_VALUE}.
     */
    public static final int DFLT_WAIT_JOBS_NUM = Integer.MAX_VALUE;

    /** Default priority attribute key (value is {@code grid.task.priority}). */
    public static final String DFLT_PRIORITY_ATTRIBUTE_KEY = "grid.task.priority";

    /** Default job priority attribute key (value is {@code grid.job.priority}). */
    public static final String DFLT_JOB_PRIORITY_ATTRIBUTE_KEY = "grid.job.priority";

    /**
     * Default priority that will be assigned if job does not have a
     * priority attribute set (value is {@code 0}).
     */
    public static final int DFLT_PRIORITY = 0;

    /** Default value on which job priority will be increased every time when job gets bumped down. */
    public static final int DFLT_STARVATION_INCREMENT = 1;

    /** Default flag for preventing starvation of lower priority jobs. */
    public static final boolean DFLT_PREVENT_STARVATION_ENABLED = true;

    /** Priority attribute key should be the same on all nodes. */
    private static final String PRIORITY_ATTRIBUTE_KEY = "gg:collision:priority";

    /** Number of jobs that can be executed in parallel. */
    private volatile int parallelJobsNum = DFLT_PARALLEL_JOBS_NUM;

    /** Wait jobs number. */
    private volatile int waitJobsNum = DFLT_WAIT_JOBS_NUM;

    /** Number of jobs that were active last time. */
    private volatile int runningCnt;

    /** Number of jobs that were waiting for execution last time. */
    private volatile int waitingCnt;

    /** Number of currently held jobs. */
    private volatile int heldCnt;

    /** */
    private String taskPriAttrKey = DFLT_PRIORITY_ATTRIBUTE_KEY;

    /** */
    private String jobPriAttrKey = DFLT_JOB_PRIORITY_ATTRIBUTE_KEY;

    /** */
    private volatile int dfltPri = DFLT_PRIORITY;

    /** */
    private volatile int starvationInc = DFLT_STARVATION_INCREMENT;

    /** */
    private volatile boolean preventStarvation = DFLT_PREVENT_STARVATION_ENABLED;

    /** Cached priority comparator instance. */
    private Comparator<GridCollisionJobContextWrapper> priComp;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public int getParallelJobsNumber() {
        return parallelJobsNum;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setParallelJobsNumber(int parallelJobsNum) {
        A.ensure(parallelJobsNum > 0,  "parallelJobsNum > 0");

        this.parallelJobsNum = parallelJobsNum;
    }

    /** {@inheritDoc} */
    @Override public int getWaitingJobsNumber() {
        return waitJobsNum;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setWaitingJobsNumber(int waitJobsNum) {
        A.ensure(waitJobsNum >= 0, "waitJobsNum >= 0");

        this.waitJobsNum = waitJobsNum;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitJobsNumber() {
        return waitingCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobsNumber() {
        return runningCnt + heldCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRunningJobsNumber() {
        return runningCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentHeldJobsNumber() {
        return heldCnt;
    }

    /**
     * Sets task priority attribute key. This key will be used to look up task
     * priorities from task context (see {@link org.apache.ignite.compute.ComputeTaskSession#getAttribute(Object)}).
     * <p>
     * If not provided, default value is {@code {@link #DFLT_PRIORITY_ATTRIBUTE_KEY}}.
     *
     * @param taskPriAttrKey Priority session attribute key.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setPriorityAttributeKey(String taskPriAttrKey) {
        this.taskPriAttrKey = taskPriAttrKey;
    }

    /**
     * Sets job priority attribute key. This key will be used to look up job
     * priorities from job context (see {@link org.apache.ignite.compute.ComputeJobContext#getAttribute(Object)}).
     * <p>
     * If not provided, default value is {@code {@link #DFLT_JOB_PRIORITY_ATTRIBUTE_KEY}}.
     *
     * @param jobPriAttrKey Job priority attribute key.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setJobPriorityAttributeKey(String jobPriAttrKey) {
        this.jobPriAttrKey = jobPriAttrKey;
    }

    /** {@inheritDoc} */
    @Override public String getPriorityAttributeKey() {
        return taskPriAttrKey;
    }

    /** {@inheritDoc} */
    @Override public String getJobPriorityAttributeKey() {
        return jobPriAttrKey;
    }

    /** {@inheritDoc} */
    @Override public int getDefaultPriority() {
        return dfltPri;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setDefaultPriority(int dfltPri) {
        this.dfltPri = dfltPri;
    }

    /** {@inheritDoc} */
    @Override public int getStarvationIncrement() {
        return starvationInc;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setStarvationIncrement(int starvationInc) {
        this.starvationInc = starvationInc;
    }

    /** {@inheritDoc} */
    @Override public boolean isStarvationPreventionEnabled() {
        return preventStarvation;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setStarvationPreventionEnabled(boolean preventStarvation) {
        this.preventStarvation = preventStarvation;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        return F.<String, Object>asMap(createSpiAttributeName(PRIORITY_ATTRIBUTE_KEY), getPriorityAttributeKey());
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        assertParameter(parallelJobsNum > 0, "parallelJobsNum > 0");
        assertParameter(waitJobsNum >= 0, "waitingJobsNum >= 0");
        assertParameter(starvationInc >= 0, "starvationInc >= 0");
        assertParameter(taskPriAttrKey != null, "taskPriAttrKey != null");
        assertParameter(jobPriAttrKey != null, "jobPriorityAttrKey != null");

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("parallelJobsNum", parallelJobsNum));
            log.debug(configInfo("taskPriAttrKey", taskPriAttrKey));
            log.debug(configInfo("jobPriorityAttrKey", jobPriAttrKey));
            log.debug(configInfo("dfltPri", dfltPri));
            log.debug(configInfo("starvationInc", starvationInc));
            log.debug(configInfo("preventStarvation", preventStarvation));
        }

        registerMBean(gridName, this, PriorityQueueCollisionSpiMBean.class);

        // Ack start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onCollision(CollisionContext ctx) {
        assert ctx != null;

        int activeSize = ctx.activeJobs().size();

        Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

        int waitSize = waitJobs.size();

        runningCnt = activeSize;
        waitingCnt = waitSize;

        heldCnt = ctx.heldJobs().size();

        int activateCnt = parallelJobsNum - activeSize;

        // Temporary snapshot of waitJobs.
        ArrayList<GridCollisionJobContextWrapper> waitSnap = slice(waitJobs, waitSize);

        boolean waitSnapSorted = false;

        if (activateCnt > 0 && waitSize > 0) {
            if (waitSize <= activateCnt) {
                for (GridCollisionJobContextWrapper cntx: waitSnap) {
                    cntx.getContext().activate();
                    waitSize--;
                }
            }
            else {
                Collections.sort(waitSnap, priorityComparator());
                waitSnapSorted = true;

                if (preventStarvation)
                    bumpPriority(waitSnap);

                // Passive list could have less then waitSize elements.
                for (int i = 0; i < activateCnt && i < waitSnap.size(); i++) {
                    waitSnap.get(i).getContext().activate();

                    waitSize--;
                }
            }
        }

        int waitJobsNum = this.waitJobsNum;

        if (waitSize > waitJobsNum) {
            int skip = waitSnap.size() - waitSize;

            if (!waitSnapSorted)
                Collections.sort(waitSnap, priorityComparator());

            int i = 0;

            for (GridCollisionJobContextWrapper wrapper : waitSnap) {
                if (++i >= skip) {
                    wrapper.getContext().cancel();

                    if (--waitSize <= waitJobsNum)
                        break;
                }
            }
        }
    }

    /**
     * Takes first n element from contexts list, wrap them and put it into a new list.
     *
     * @param src Collection of collision contexts to take elements from.
     * @param num Number of elements to take.
     * @return A new list, containing @code min(num, source.size()) elements.
     */
    private static ArrayList<GridCollisionJobContextWrapper> slice(Collection<CollisionJobContext> src, int num) {
        ArrayList<GridCollisionJobContextWrapper> slice = new ArrayList<>();

        Iterator<CollisionJobContext> iter = src.iterator();

        for (int i = 0; i < num && iter.hasNext(); i++)
            slice.add(new GridCollisionJobContextWrapper(iter.next(), i));

        return slice;
    }

    /**
     * Increases priority if job has bumped down.
     *
     * @param jobs Ordered collection of collision contexts for jobs that are currently waiting
     *      for execution.
     */
    private void bumpPriority(List<GridCollisionJobContextWrapper> jobs) {
        int starvationInc = this.starvationInc;

        for (int i = 0; i < jobs.size(); i++) {
            GridCollisionJobContextWrapper wrapper = jobs.get(i);

            if (i > wrapper.originalIndex())
                wrapper.getContext().getJobContext()
                    .setAttribute(jobPriAttrKey, getJobPriority(wrapper.getContext()) + starvationInc);
        }
    }

    /**
     * Gets job priority. At first tries to get from job context. If job context has no priority,
     * then tries to get from task session. If task session has no priority default one will be used.
     *
     * @param ctx Collision job context.
     * @return Job priority.
     */
    private int getJobPriority(CollisionJobContext ctx) {
        assert ctx != null;

        Integer p = null;

        ComputeJobContext jctx = ctx.getJobContext();

        try {
            p = (Integer)jctx.getAttribute(jobPriAttrKey);
        }
        catch (ClassCastException e) {
            LT.error(log, e, "Type of job context priority attribute '" + jobPriAttrKey +
                "' is not java.lang.Integer [type=" + jctx.getAttribute(jobPriAttrKey).getClass() + ']');
        }

        if (p == null) {
            ComputeTaskSession ses = ctx.getTaskSession();

            try {
                p = (Integer)ses.getAttribute(taskPriAttrKey);
            }
            catch (ClassCastException e) {
                LT.error(log, e, "Type of task session priority attribute '" + taskPriAttrKey +
                    "' is not java.lang.Integer [type=" + ses.getAttribute(taskPriAttrKey).getClass() + ']');
            }

            if (p == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed get priority from job context attribute '" + jobPriAttrKey +
                        "' and task session attribute '" + taskPriAttrKey + "' (will use default priority): " +
                        dfltPri);
                }

                p = dfltPri;
            }
        }

        assert p != null;

        return p;
    }

    /** {@inheritDoc} */
    @Override protected List<String> getConsistentAttributeNames() {
        return Collections.singletonList(createSpiAttributeName(PRIORITY_ATTRIBUTE_KEY));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PriorityQueueCollisionSpi.class, this);
    }

    /**
     * Returns (possibly shared) comparator fo sorting GridCollisionJobContextWrapper
     * by priority.
     *
     * @return Comparator for priority sorting.
     */
    private Comparator<GridCollisionJobContextWrapper> priorityComparator() {
        if (priComp == null)
            priComp = new PriorityGridCollisionJobContextComparator();

        return priComp;
    }

    /**
     * Comparator for by priority comparison of collision contexts.
     */
    private class PriorityGridCollisionJobContextComparator implements Comparator<GridCollisionJobContextWrapper>,Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(GridCollisionJobContextWrapper o1, GridCollisionJobContextWrapper o2) {
            int p1 = getJobPriority(o1.getContext());
            int p2 = getJobPriority(o2.getContext());

            return p1 < p2 ? 1 : p1 == p2 ? 0 : -1;
        }
    }

    /**
     * Wrapper class to keep original collision context position.
     */
    private static class GridCollisionJobContextWrapper {
        /** Wrapped collision context. */
        private final CollisionJobContext ctx;

        /** Index of wrapped context in original collection. */
        private final int originalIdx;

        /**
         * @param ctx Wrapped collision context.
         * @param originalIdx Index of wrapped context in original collection.
         */
        private GridCollisionJobContextWrapper(CollisionJobContext ctx, int originalIdx) {
            this.ctx = ctx;
            this.originalIdx = originalIdx;
        }

        /**
         * @return Wrapped collision context.
         */
        public CollisionJobContext getContext() {
            return ctx;
        }

        /**
         * @return Index of wrapped context in original collection.
         */
        public int originalIndex() {
            return originalIdx;
        }
    }
}