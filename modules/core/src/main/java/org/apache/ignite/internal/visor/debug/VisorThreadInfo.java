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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for Visor {@link ThreadInfo}.
 */
public class VisorThreadInfo extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int MAX_FRAMES = 8;

    /** Thread name. */
    private String name;

    /** Thread ID. */
    private long id;

    /** Thread state. */
    private Thread.State state;

    /** Lock information. */
    private VisorThreadLockInfo lock;

    /** Lock name. */
    private String lockName;

    /** Lock owner thread ID. */
    private long lockOwnerId;

    /** Lock owner name. */
    private String lockOwnerName;

    /** Thread executing native code. */
    private boolean inNative;

    /** Thread is suspended. */
    private boolean suspended;

    /** Waited count. */
    private long waitedCnt;

    /** Waited time. */
    private long waitedTime;

    /** Blocked count. */
    private long blockedCnt;

    /** Blocked time. */
    private long blockedTime;

    /** Stack trace. */
    private List<StackTraceElement> stackTrace;

    /** Locks info. */
    private List<VisorThreadLockInfo> locks;

    /** Locked monitors. */
    private List<VisorThreadMonitorInfo> lockedMonitors;

    /**
     * Default constructor.
     */
    public VisorThreadInfo() {
        // No-op.
    }

    /**
     * Create data transfer object for given thread info.
     *
     * @param ti Thread info.
     */
    public VisorThreadInfo(ThreadInfo ti) {
        assert ti != null;

        name = ti.getThreadName();
        id = ti.getThreadId();
        state = ti.getThreadState();
        lock = ti.getLockInfo() != null ? new VisorThreadLockInfo(ti.getLockInfo()) : null;
        lockName =ti.getLockName();
        lockOwnerId = ti.getLockOwnerId();
        lockOwnerName = ti.getLockOwnerName();
        inNative = ti.isInNative();
        suspended = ti.isSuspended();
        waitedCnt = ti.getWaitedCount();
        waitedTime = ti.getWaitedTime();
        blockedCnt = ti.getBlockedCount();
        blockedTime = ti.getBlockedTime();
        stackTrace = Arrays.asList(ti.getStackTrace());

        locks = ti.getLockedSynchronizers() != null ?
            new ArrayList<VisorThreadLockInfo>(ti.getLockedSynchronizers().length) : null;

        if (ti.getLockedSynchronizers() != null)
            for (int i = 0; i < ti.getLockedSynchronizers().length; i++)
                locks.add(new VisorThreadLockInfo(ti.getLockedSynchronizers()[i]));

        lockedMonitors = ti.getLockedMonitors() != null ?
            new ArrayList<VisorThreadMonitorInfo>(ti.getLockedMonitors().length) : null;

        if (ti.getLockedMonitors() != null)
            for (int i = 0; i < ti.getLockedMonitors().length; i++)
                lockedMonitors.add(new VisorThreadMonitorInfo(ti.getLockedMonitors()[i]));
    }

    /**
     * @return Thread name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Thread ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @return Thread state.
     */
    public Thread.State getState() {
        return state;
    }

    /**
     * @return Lock information.
     */
    public VisorThreadLockInfo getLock() {
        return lock;
    }

    /**
     * @return Lock name.
     */
    public String getLockName() {
        return lockName;
    }

    /**
     * @return Lock owner thread ID.
     */
    public long getLockOwnerId() {
        return lockOwnerId;
    }

    /**
     * @return Lock owner name.
     */
    public String getLockOwnerName() {
        return lockOwnerName;
    }

    /**
     * @return Thread executing native code.
     */
    public boolean isInNative() {
        return inNative;
    }

    /**
     * @return Thread is suspended.
     */
    public boolean isSuspended() {
        return suspended;
    }

    /**
     * @return Waited count.
     */
    public long getWaitedCount() {
        return waitedCnt;
    }

    /**
     * @return Waited time.
     */
    public long getWaitedTime() {
        return waitedTime;
    }

    /**
     * @return Blocked count.
     */
    public long getBlockedCount() {
        return blockedCnt;
    }

    /**
     * @return Blocked time.
     */
    public long getBlockedTime() {
        return blockedTime;
    }

    /**
     * @return Stack trace.
     */
    public List<StackTraceElement> getStackTrace() {
        return stackTrace;
    }

    /**
     * @return Locks info.
     */
    public List<VisorThreadLockInfo> getLocks() {
        return locks;
    }

    /**
     * @return Locked monitors.
     */
    public List<VisorThreadMonitorInfo> getLockedMonitors() {
        return lockedMonitors;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeLong(id);
        U.writeString(out, state.toString());
        out.writeObject(lock);
        U.writeString(out, lockName);
        out.writeLong(lockOwnerId);
        U.writeString(out, lockOwnerName);
        out.writeBoolean(inNative);
        out.writeBoolean(suspended);
        out.writeLong(waitedCnt);
        out.writeLong(waitedTime);
        out.writeLong(blockedCnt);
        out.writeLong(blockedTime);
        U.writeCollection(out, stackTrace);
        U.writeCollection(out, locks);
        U.writeCollection(out, lockedMonitors);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        id = in.readLong();

        String statePresentation = U.readString(in);

        if (statePresentation != null)
            state = Enum.valueOf(Thread.State.class, statePresentation);

        lock = (VisorThreadLockInfo)in.readObject();
        lockName = U.readString(in);
        lockOwnerId = in.readLong();
        lockOwnerName = U.readString(in);
        inNative = in.readBoolean();
        suspended = in.readBoolean();
        waitedCnt = in.readLong();
        waitedTime = in.readLong();
        blockedCnt = in.readLong();
        blockedTime = in.readLong();
        stackTrace = U.readList(in);
        locks = U.readList(in);
        lockedMonitors = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder(512);

        sb.append('"').append(name).append('"').append(" Id=").append(id).append(' ').append(state);

        if (lockName != null)
            sb.append(" on ").append(lockName);

        if (lockOwnerName != null)
            sb.append(" owned by \"").append(lockOwnerName).append("\" Id=").append(lockOwnerId);

        if (suspended)
            sb.append(" (suspended)");

        if (inNative)
            sb.append(" (in native)");

        sb.append('\n');

        int maxFrames = Math.min(stackTrace.size(), MAX_FRAMES);

        for (int i = 0; i < maxFrames; i++) {
            StackTraceElement ste = stackTrace.get(i);

            sb.append("\tat ").append(ste).append('\n');

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
                if (mi.getStackDepth() == i)
                    sb.append("\t-  locked ").append(mi).append('\n');
            }
        }

        if (maxFrames < stackTrace.size())
            sb.append("\t...").append('\n');

        if (!F.isEmpty(locks)) {
            sb.append("\n\tNumber of locked synchronizers = ").append(locks.size()).append('\n');

            for (VisorThreadLockInfo li : locks)
                sb.append("\t- ").append(li).append('\n');
        }

        sb.append('\n');

        return sb.toString();
    }
}
