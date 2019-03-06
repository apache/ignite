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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class that allows to ignore back-pressure control for threads that are processing messages.
 */
public class GridNioBackPressureControl {
    /** Thread local flag indicating that thread is processing message. */
    private static ThreadLocal<Holder> threadProcMsg = new ThreadLocal<Holder>() {
        @Override protected Holder initialValue() {
            return new Holder();
        }
    };

    /**
     * @return Flag indicating whether current thread is processing message.
     */
    public static boolean threadProcessingMessage() {
        return threadProcMsg.get().procMsg;
    }

    /**
     * @param processing Flag indicating whether current thread is processing message.
     * @param tracker Thread local back pressure tracker of messages, associated with one connection.
     */
    public static void threadProcessingMessage(boolean processing, @Nullable IgniteRunnable tracker) {
        Holder holder = threadProcMsg.get();

        holder.procMsg = processing;
        holder.tracker = tracker;
    }

    /**
     * @return Thread local back pressure tracker of messages, associated with one connection.
     */
    @Nullable public static IgniteRunnable threadTracker() {
        return threadProcMsg.get().tracker;
    }

    /**
     *
     */
    private static class Holder {
        /** Process message. */
        private boolean procMsg;

        /** Tracker. */
        private IgniteRunnable tracker;
    }
}
