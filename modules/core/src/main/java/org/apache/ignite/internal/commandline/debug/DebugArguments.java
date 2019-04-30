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

package org.apache.ignite.internal.commandline.debug;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.visor.debug.VisorDumpDebugInfoArg;

/**
 * This class contains all possible arguments after parsing debug command input.
 */
public class DebugArguments {
    /** Command. */
    private DebugCommand cmd;
    /** */
    private List<Long> pageIds;

    /** */
    private String pathToDump;

    /** */
    private Collection<VisorDumpDebugInfoArg.DumpAction> dumpActions;

    /**
     * @param cmd Dump command.
     * @param pageIds Page ids.
     * @param pathToDump Path to dump.
     * @param dumpActions Dump debug info actions.
     */
    public DebugArguments(DebugCommand cmd, List<Long> pageIds, String pathToDump,
        Collection<VisorDumpDebugInfoArg.DumpAction> dumpActions) {
        this.cmd = cmd;
        this.pageIds = pageIds;
        this.pathToDump = pathToDump;
        this.dumpActions = dumpActions;
    }

    /**
     * @return Debug command.
     */
    public DebugCommand getCmd() {
        return cmd;
    }

    /**
     * @return Page ids.
     */
    public List<Long> getPageIds() {
        return pageIds;
    }

    /**
     * @return Path to dump debug info.
     */
    public String getPathToDump() {
        return pathToDump;
    }

    /**
     * @return Dump debug info actions.
     */
    public Collection<VisorDumpDebugInfoArg.DumpAction> getDumpActions() {
        return dumpActions;
    }

    /**
     * Builder of {@link DebugArguments}.
     */
    public static class Builder {
        /** Command. */
        private DebugCommand cmd;
        /** */
        private List<Long> pageIds;

        /** */
        private String fileToDump;

        private Set<VisorDumpDebugInfoArg.DumpAction> dumpActions = new HashSet<>();

        /**
         * @param cmd Command.
         */
        public Builder(DebugCommand cmd) {
            this.cmd = cmd;
        }

        /**
         * @param pageIds List of page id for searching.
         * @return This instance for chaining.
         */
        public Builder withPageIds(List<Long> pageIds) {
            this.pageIds = pageIds;

            return this;
        }

        /**
         * @param pathToDump Path to dump debug info.
         * @return This instance for chaining.
         */
        public Builder withPathToDump(String pathToDump) {
            this.fileToDump = pathToDump;

            return this;
        }

        /**
         * @param dumpAction Dump one more debug action.
         * @return This instance for chaining.
         */
        public Builder addDumpAction(VisorDumpDebugInfoArg.DumpAction dumpAction) {
            this.dumpActions.add(dumpAction);

            return this;
        }

        /**
         * @return {@link DebugArguments}.
         */
        public DebugArguments build() {
            return new DebugArguments(cmd, pageIds, fileToDump, dumpActions);
        }
    }
}
