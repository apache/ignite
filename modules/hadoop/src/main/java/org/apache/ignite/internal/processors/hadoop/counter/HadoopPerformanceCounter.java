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

package org.apache.ignite.internal.processors.hadoop.counter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.JOB_SUBMISSION_START_TS_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.REQ_NEW_JOBID_TS_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.RESPONSE_NEW_JOBID_TS_PROPERTY;

/**
 * Counter for the job statistics accumulation.
 */
public class HadoopPerformanceCounter extends HadoopCounterAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** The group name for this counter. */
    private static final String GROUP_NAME = "SYSTEM";

    /** The counter name for this counter. */
    private static final String COUNTER_NAME = "PERFORMANCE";

    /** Events collections. */
    private Collection<T2<String,Long>> evts = new ArrayList<>();

    /** Node id to insert into the event info. */
    private UUID nodeId;

    /** */
    private int reducerNum;

    /** */
    private volatile Long firstShuffleMsg;

    /** */
    private volatile Long lastShuffleMsg;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public HadoopPerformanceCounter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param grp Group name.
     * @param name Counter name.
     */
    public HadoopPerformanceCounter(String grp, String name) {
        super(grp, name);
    }

    /**
     * Constructor to create instance to use this as helper.
     *
     * @param nodeId Id of the work node.
     */
    public HadoopPerformanceCounter(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        U.writeCollection(out, evts);
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        try {
            evts = U.readCollection(in);
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void merge(HadoopCounter cntr) {
        evts.addAll(((HadoopPerformanceCounter)cntr).evts);
    }

    /**
     * Gets the events collection.
     *
     * @return Collection of event.
     */
    public Collection<T2<String, Long>> evts() {
        return evts;
    }

    /**
     * Generate name that consists of some event information.
     *
     * @param info Task info.
     * @param evtType The type of the event.
     * @return String contains necessary event information.
     */
    private String eventName(HadoopTaskInfo info, String evtType) {
        return eventName(info.type().toString(), info.taskNumber(), evtType);
    }

    /**
     * Generate name that consists of some event information.
     *
     * @param taskType Task type.
     * @param taskNum Number of the task.
     * @param evtType The type of the event.
     * @return String contains necessary event information.
     */
    private String eventName(String taskType, int taskNum, String evtType) {
        assert nodeId != null;

        return taskType + " " + taskNum + " " + evtType + " " + nodeId;
    }

    /**
     * Adds event of the task submission (task instance creation).
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskSubmit(HadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "submit"), ts));
    }

    /**
     * Adds event of the task preparation.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskPrepare(HadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "prepare"), ts));
    }

    /**
     * Adds event of the task finish.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskFinish(HadoopTaskInfo info, long ts) {
        if (info.type() == HadoopTaskType.REDUCE && lastShuffleMsg != null) {
            evts.add(new T2<>(eventName("SHUFFLE", reducerNum, "start"), firstShuffleMsg));
            evts.add(new T2<>(eventName("SHUFFLE", reducerNum, "finish"), lastShuffleMsg));

            lastShuffleMsg = null;
        }

        evts.add(new T2<>(eventName(info, "finish"), ts));
    }

    /**
     * Adds event of the task run.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskStart(HadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "start"), ts));
    }

    /**
     * Adds event of the job preparation.
     *
     * @param ts Timestamp of the event.
     */
    public void onJobPrepare(long ts) {
        assert nodeId != null;

        evts.add(new T2<>("JOB prepare " + nodeId, ts));
    }

    /**
     * Adds event of the job start.
     *
     * @param ts Timestamp of the event.
     */
    public void onJobStart(long ts) {
        assert nodeId != null;

        evts.add(new T2<>("JOB start " + nodeId, ts));
    }

    /**
     * Adds client submission events from job info.
     *
     * @param info Job info.
     */
    public void clientSubmissionEvents(HadoopJobInfo info) {
        assert nodeId != null;

        addEventFromProperty("JOB requestId", info, REQ_NEW_JOBID_TS_PROPERTY);
        addEventFromProperty("JOB responseId", info, RESPONSE_NEW_JOBID_TS_PROPERTY);
        addEventFromProperty("JOB submit", info, JOB_SUBMISSION_START_TS_PROPERTY);
    }

    /**
     * Adds event with timestamp from some property in job info.
     *
     * @param evt Event type and phase.
     * @param info Job info.
     * @param propName Property name to get timestamp.
     */
    private void addEventFromProperty(String evt, HadoopJobInfo info, String propName) {
        String val = info.property(propName);

        if (!F.isEmpty(val)) {
            try {
                evts.add(new T2<>(evt + " " + nodeId, Long.parseLong(val)));
            }
            catch (NumberFormatException e) {
                throw new IllegalStateException("Invalid value '" + val + "' of property '" + propName + "'", e);
            }
        }
    }

    /**
     * Registers shuffle message event.
     *
     * @param reducerNum Number of reducer that receives the data.
     * @param ts Timestamp of the event.
     */
    public void onShuffleMessage(int reducerNum, long ts) {
        this.reducerNum = reducerNum;

        if (firstShuffleMsg == null)
            firstShuffleMsg = ts;

        lastShuffleMsg = ts;
    }

    /**
     * Gets system predefined performance counter from the HadoopCounters object.
     *
     * @param cntrs HadoopCounters object.
     * @param nodeId Node id for methods that adds events. It may be null if you don't use ones.
     * @return Predefined performance counter.
     */
    public static HadoopPerformanceCounter getCounter(HadoopCounters cntrs, @Nullable UUID nodeId) {
        HadoopPerformanceCounter cntr = cntrs.counter(GROUP_NAME, COUNTER_NAME, HadoopPerformanceCounter.class);

        if (nodeId != null)
            cntr.nodeId(nodeId);

        return cntrs.counter(GROUP_NAME, COUNTER_NAME, HadoopPerformanceCounter.class);
    }

    /**
     * Sets the nodeId field.
     *
     * @param nodeId Node id.
     */
    private void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }
}