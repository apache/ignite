/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

/**
 * List of trace names used in appropriate sub-systems.
 */
public class Traces {
    /**
     * Discovery traces.
     */
    public static class Discovery {
        /** Node join request. */
        public static final String NODE_JOIN_REQUEST = "discovery.node.join.request";
        /** Node join add. */
        public static final String NODE_JOIN_ADD = "discovery.node.join.add";
        /** Node join finish. */
        public static final String NODE_JOIN_FINISH = "discovery.node.join.finish";
        /** Node failed. */
        public static final String NODE_FAILED = "discovery.node.failed";
        /** Node left. */
        public static final String NODE_LEFT = "discovery.node.left";
        /** Custom event. */
        public static final String CUSTOM_EVENT = "discovery.custom.event";

        /**
         * Default constructor.
         */
        private Discovery() {
        }
    }

    /**
     * Exchange traces.
     */
    public static class Exchange {
        /** Exchange future. */
        public static final String EXCHANGE_FUTURE = "exchange.future";

        /**
         * Default constructor.
         */
        private Exchange() {
        }
    }

    /**
     * Communication traces.
     */
    public static class Communication {
        /**
         * Default constructor.
         */
        private Communication() {
        }

        /** Job execution request. */
        public static final String JOB_EXECUTE_REQUEST = "communication.job.execute.request";
        /** Job execution response. */
        public static final String JOB_EXECUTE_RESPONSE = "communication.job.execute.response";
        /** Socket write action. */
        public static final String SOCKET_WRITE = "socket.write";
        /** Socket read action. */
        public static final String SOCKET_READ = "socket.read";
        /** Process regular. */
        public static final String REGULAR_PROCESS = "process.regular";
        /** Process ordered. */
        public static final String ORDERED_PROCESS = "process.ordered";
    }

    /**
     * Default constructor.
     */
    private Traces() {
    }
}
