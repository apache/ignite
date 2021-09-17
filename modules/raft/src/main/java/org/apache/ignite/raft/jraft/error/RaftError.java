/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Raft error code.
 */
public enum RaftError {

    /**
     * Unknown error
     */
    UNKNOWN(-1),

    /**
     * Success, no error.
     */
    SUCCESS(0),

    /**
     * <pre>
     * All Kinds of Timeout(Including Election_timeout, Timeout_now, Stepdown_timeout)
     * </pre>
     * * <code>ERAFTTIMEDOUT = 10001;</code>
     */
    ERAFTTIMEDOUT(10001),

    /**
     * <pre>
     * Bad User State Machine
     * </pre>
     * * <code>ESTATEMACHINE = 10002;</code>
     */
    ESTATEMACHINE(10002),

    /**
     * <pre>
     * Catchup Failed
     * </pre>
     * * <code>ECATCHUP = 10003;</code>
     */
    ECATCHUP(10003),

    /**
     * <pre>
     * Trigger step_down(Not All)
     * </pre>
     * * <code>ELEADERREMOVED = 10004;</code>
     */
    ELEADERREMOVED(10004),

    /**
     * <pre>
     * Leader Is Not In The New Configuration
     * </pre>
     * * <code>ESETPEER = 10005;</code>
     */
    ESETPEER(10005),

    /**
     * <pre>
     * Shut_down
     * </pre>
     * * <code>ENODESHUTDOWN = 10006;</code>
     */
    ENODESHUTDOWN(10006),

    /**
     * <pre>
     * Receive Higher Term Requests
     * </pre>
     * * <code>EHIGHERTERMREQUEST = 10007;</code>
     */
    EHIGHERTERMREQUEST(10007),

    /**
     * <pre>
     * Receive Higher Term Response
     * </pre>
     * * <code>EHIGHERTERMRESPONSE = 10008;</code>
     */
    EHIGHERTERMRESPONSE(10008),

    /**
     * <pre>
     * Node Is In Error
     * </pre>
     * * <code>EBADNODE = 10009;</code>
     */
    EBADNODE(10009),

    /**
     * <pre>
     * Node Votes For Some Candidate
     * </pre>
     * * <code>EVOTEFORCANDIDATE = 10010;</code>
     */
    EVOTEFORCANDIDATE(10010),

    /**
     * <pre>
     * Follower(without leader) or Candidate Receives
     * </pre>
     * * <code>ENEWLEADER = 10011;</code>
     */
    ENEWLEADER(10011),

    /**
     * <pre>
     * Append_entries/Install_snapshot Request from a new leader
     * </pre>
     * * <code>ELEADERCONFLICT = 10012;</code>
     */
    ELEADERCONFLICT(10012),

    /**
     * <pre>
     * Trigger on_leader_stop
     * </pre>
     * * <code>ETRANSFERLEADERSHIP = 10013;</code>
     */
    ETRANSFERLEADERSHIP(10013),

    /**
     * <pre>
     * The log at the given index is deleted
     * </pre>
     * * <code>ELOGDELETED = 10014;</code>
     */
    ELOGDELETED(10014),

    /**
     * <pre>
     * No available user log to read
     * </pre>
     * * <code>ENOMOREUSERLOG = 10015;</code>
     */
    ENOMOREUSERLOG(10015),

    /* other non-raft error codes 1000~10000 */
    /**
     * Invalid rpc request
     */
    EREQUEST(1000),

    /**
     * Task is stopped
     */
    ESTOP(1001),

    /**
     * Retry again
     */
    EAGAIN(1002),

    /**
     * Interrupted
     */
    EINTR(1003),

    /**
     * Internal exception
     */
    EINTERNAL(1004),

    /**
     * Task is canceled
     */
    ECANCELED(1005),

    /**
     * Host is down
     */
    EHOSTDOWN(1006),

    /**
     * Service is shutdown
     */
    ESHUTDOWN(1007),

    /**
     * Permission issue
     */
    EPERM(1008),

    /**
     * Server is in busy state
     */
    EBUSY(1009),

    /**
     * Timed out
     */
    ETIMEDOUT(1010),

    /**
     * Data is stale
     */
    ESTALE(1011),

    /**
     * Something not found
     */
    ENOENT(1012),

    /**
     * File/folder already exists
     */
    EEXISTS(1013),

    /**
     * IO error
     */
    EIO(1014),

    /**
     * Invalid value.
     */
    EINVAL(1015),

    /**
     * Permission denied
     */
    EACCES(1016);

    private static final Map<Integer, RaftError> RAFT_ERROR_MAP = new HashMap<>();

    static {
        for (final RaftError error : RaftError.values()) {
            RAFT_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    public final int getNumber() {
        return this.value;
    }

    public static RaftError forNumber(final int value) {
        return RAFT_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public static String describeCode(final int code) {
        RaftError e = forNumber(code);
        return e != null ? e.name() : "<Unknown:" + code + ">";
    }

    private final int value;

    RaftError(final int value) {
        this.value = value;
    }
}
