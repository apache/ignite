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

package org.apache.ignite.internal.visor.debug;

import java.io.Serializable;
import java.lang.management.ThreadInfo;

/**
 * Data transfer object for Visor {@link ThreadInfo}.
 */
public class VisorThreadInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int MAX_FRAMES = 8;

    /** Thread name. */
    private final String name;

    /** Thread ID. */
    private final Long id;

    /** Thread state. */
    private final Thread.State state;

    /** Lock information. */
    private final VisorThreadLockInfo lock;

    /** Lock name. */
    private final String lockName;

    /** Lock owner thread ID. */
    private final Long lockOwnerId;

    /** Lock owner name. */
    private final String lockOwnerName;

    /** Thread executing native code. */
    private final Boolean inNative;

    /** Thread is suspended. */
    private final Boolean suspended;

    /** Waited count. */
    private final Long waitedCnt;

    /** Waited time. */
    private final Long waitedTime;

    /** Blocked count. */
    private final Long blockedCnt;

    /** Blocked time. */
    private final Long blockedTime;

    /** Stack trace. */
    private final StackTraceElement[] stackTrace;

    /** Locks info. */
    private final VisorThreadLockInfo[] locks;

    /** Locked monitors. */
    private final VisorThreadMonitorInfo[] lockedMonitors;

    /** Create thread info with given parameters. */
    public VisorThreadInfo(String name,
        Long id,
        Thread.State state,
        VisorThreadLockInfo lock,
        String lockName,
        Long lockOwnerId,
        String lockOwnerName,
        Boolean inNative,
        Boolean suspended,
        Long waitedCnt,
        Long waitedTime,
        Long blockedCnt,
        Long blockedTime,
        StackTraceElement[] stackTrace,
        VisorThreadLockInfo[] locks,
        VisorThreadMonitorInfo[] lockedMonitors
    ) {
        this.name = name;
        this.id = id;
        this.state = state;
        this.lock = lock;
        this.lockName = lockName;
        this.lockOwnerId = lockOwnerId;
        this.lockOwnerName = lockOwnerName;
        this.inNative = inNative;
        this.suspended = suspended;
        this.waitedCnt = waitedCnt;
        this.waitedTime = waitedTime;
        this.blockedCnt = blockedCnt;
        this.blockedTime = blockedTime;
        this.stackTrace = stackTrace;
        this.locks = locks;
        this.lockedMonitors = lockedMonitors;
    }

    /** Create data transfer object for given thread info. */
    public static VisorThreadInfo from(ThreadInfo ti) {
        assert ti != null;

        VisorThreadLockInfo[] linfos = ti.getLockedSynchronizers() != null ?
            new VisorThreadLockInfo[ti.getLockedSynchronizers().length] : null;

        if (ti.getLockedSynchronizers() != null)
            for (int i = 0; i < ti.getLockedSynchronizers().length; i++)
                linfos[i] = VisorThreadLockInfo.from(ti.getLockedSynchronizers()[i]);

        VisorThreadMonitorInfo[] minfos = ti.getLockedMonitors() != null ?
            new VisorThreadMonitorInfo[ti.getLockedMonitors().length] : null;

        if (ti.getLockedMonitors() != null)
            for (int i = 0; i < ti.getLockedMonitors().length; i++)
                minfos[i] = VisorThreadMonitorInfo.from(ti.getLockedMonitors()[i]);

        return new VisorThreadInfo(ti.getThreadName(),
            ti.getThreadId(),
            ti.getThreadState(),
            ti.getLockInfo() != null ? VisorThreadLockInfo.from(ti.getLockInfo()) : null,
            ti.getLockName(),
            ti.getLockOwnerId(),
            ti.getLockOwnerName(),
            ti.isInNative(),
            ti.isSuspended(),
            ti.getWaitedCount(),
            ti.getWaitedTime(),
            ti.getBlockedCount(),
            ti.getBlockedTime(),
            ti.getStackTrace(),
            linfos,
            minfos
        );
    }

    /**
     * @return Thread name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Thread ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Thread state.
     */
    public Thread.State state() {
        return state;
    }

    /**
     * @return Lock information.
     */
    public VisorThreadLockInfo lock() {
        return lock;
    }

    /**
     * @return Lock name.
     */
    public String lockName() {
        return lockName;
    }

    /**
     * @return Lock owner thread ID.
     */
    public Long lockOwnerId() {
        return lockOwnerId;
    }

    /**
     * @return Lock owner name.
     */
    public String lockOwnerName() {
        return lockOwnerName;
    }

    /**
     * @return Thread executing native code.
     */
    public Boolean inNative() {
        return inNative;
    }

    /**
     * @return Thread is suspended.
     */
    public Boolean suspended() {
        return suspended;
    }

    /**
     * @return Waited count.
     */
    public Long waitedCount() {
        return waitedCnt;
    }

    /**
     * @return Waited time.
     */
    public Long waitedTime() {
        return waitedTime;
    }

    /**
     * @return Blocked count.
     */
    public Long blockedCount() {
        return blockedCnt;
    }

    /**
     * @return Blocked time.
     */
    public Long blockedTime() {
        return blockedTime;
    }

    /**
     * @return Stack trace.
     */
    public StackTraceElement[] stackTrace() {
        return stackTrace;
    }

    /**
     * @return Locks info.
     */
    public VisorThreadLockInfo[] locks() {
        return locks;
    }

    /**
     * @return Locked monitors.
     */
    public VisorThreadMonitorInfo[] lockedMonitors() {
        return lockedMonitors;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("\"" + name + "\"" + " Id=" + id + " " + state);

        if (lockName != null)
            sb.append(" on ").append(lockName);

        if (lockOwnerName != null)
            sb.append(" owned by \"").append(lockOwnerName).append("\" Id=").append(lockOwnerId);

        if (suspended)
            sb.append(" (suspended)");

        if (inNative)
            sb.append(" (in native)");

        sb.append('\n');

        int maxFrames = Math.min(stackTrace.length, MAX_FRAMES);

        for (int i = 0; i < maxFrames; i++) {
            StackTraceElement ste = stackTrace[i];

            sb.append("\tat ").append(ste.toString()).append('\n');

            if (i == 0 && lock != null) {
                switch (state) {
                    case BLOCKED:
                        sb.append("\t-  blocked on ").append(lock).append('\n');
                        break;

                    case WAITING:
                        sb.append("\t-  waiting on ").append(lock).append('\n');
                        break;

                    case TIMED_WAITING:
                        sb.append("\t-  waiting on ").append(lock).append('\n');
                        break;

                    default:
                }
            }

            for (VisorThreadMonitorInfo mi : lockedMonitors) {
                if (mi.stackDepth() == i)
                    sb.append("\t-  locked ").append(mi).append('\n');
            }
        }

        if (maxFrames < stackTrace.length)
            sb.append("\t...").append('\n');

        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = ").append(locks.length).append('\n');

            for (VisorThreadLockInfo li : locks)
                sb.append("\t- ").append(li).append('\n');
        }

        sb.append('\n');

        return sb.toString();
    }
}