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

import java.lang.management.MonitorInfo;

/**
 * Data transfer object for {@link MonitorInfo}.
 */
public class VisorThreadMonitorInfo extends VisorThreadLockInfo {
    /** */
    private static final long serialVersionUID = 0L;

    /** Stack depth. */
    private final Integer stackDepth;

    /** Stack frame. */
    private final StackTraceElement stackFrame;

    /**
     * Create thread monitor info with given parameters.
     *
     * @param clsName Fully qualified name of the class of the lock object.
     * @param identityHashCode Identity hash code of the lock object.
     * @param stackDepth Depth in the stack trace where the object monitor was locked.
     * @param stackFrame Stack frame that locked the object monitor.
     */
    public VisorThreadMonitorInfo(String clsName, Integer identityHashCode, Integer stackDepth,
        StackTraceElement stackFrame) {
        super(clsName, identityHashCode);

        this.stackDepth = stackDepth;
        this.stackFrame = stackFrame;
    }

    /** Create data transfer object for given monitor info. */
    public static VisorThreadMonitorInfo from(MonitorInfo mi) {
        assert mi != null;

        return new VisorThreadMonitorInfo(mi.getClassName(), mi.getIdentityHashCode(), mi.getLockedStackDepth(),
            mi.getLockedStackFrame());
    }

    /**
     * @return Stack depth.
     */
    public Integer stackDepth() {
        return stackDepth;
    }

    /**
     * @return Stack frame.
     */
    public StackTraceElement stackFrame() {
        return stackFrame;
    }
}