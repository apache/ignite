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

package org.apache.ignite.internal.management.performancestatistics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Command to manage performance statistics. */
public class PerformanceStatisticsCommand extends CommandRegistryImpl {
    /** */
    public PerformanceStatisticsCommand() {
        super(
            new PerformanceStatisticsStartCommand(),
            new PerformanceStatisticsStopCommand(),
            new PerformanceStatisticsRotateCommand(),
            new PerformanceStatisticsStatusCommand()
        );
    }

    /** */
    public static class PerformanceStatisticsStartCommandArg extends PerformanceStatisticsStatusCommandArg {
        /** */
        private static final long serialVersionUID = 0;

        /** System views to get content from. */
        @Argument(
            optional = true,
            description = "Comma-separated list of the system view names which should be collected. " +
                " Both \"SQL\" and \"Java\" styles of system view name are supported" +
                " (e.g. SQL_TABLES and sql.tables will be handled similarly)",
            example = "view1,view2,.."
        )
        private String[] systemViews;

        /** */
        @Argument(description = "Collect all registered system views", optional = true)
        private boolean allSystemViews;


        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeArray(out, systemViews);
            out.writeBoolean(allSystemViews);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            systemViews = U.readArray(in, String.class);
            allSystemViews = in.readBoolean();
        }

        /** */
        public String[] systemViews() {
            return systemViews;
        }

        /** */
        public void systemViews(String[] systemViews) {
            this.systemViews = systemViews;
        }

        /** */
        public boolean allSystemViews() {
            return allSystemViews;
        }

        /** */
        public void allSystemViews(boolean allSystemViews) {
            this.allSystemViews = allSystemViews;
        }

    }

    /** */
    public static class PerformanceStatisticsStopCommandArg extends PerformanceStatisticsStatusCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PerformanceStatisticsRotateCommandArg extends PerformanceStatisticsStatusCommandArg {
        /** */
        private static final long serialVersionUID = 0;
    }

    /** */
    public static class PerformanceStatisticsStatusCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}
