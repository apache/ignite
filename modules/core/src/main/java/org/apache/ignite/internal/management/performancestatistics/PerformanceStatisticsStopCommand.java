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

import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsStopCommandArg;

/** */
public class PerformanceStatisticsStopCommand implements ComputeCommand<IgniteDataTransferObject, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Stop collecting performance statistics in the cluster";
    }

    /** {@inheritDoc} */
    @Override public Class<PerformanceStatisticsStopCommandArg> argClass() {
        return PerformanceStatisticsStopCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<PerformanceStatisticsTask> taskClass() {
        return PerformanceStatisticsTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(IgniteDataTransferObject arg, String res, Consumer<String> printer) {
        printer.accept(res);
    }
}
