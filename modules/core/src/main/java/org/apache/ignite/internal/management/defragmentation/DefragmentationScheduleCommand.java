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

package org.apache.ignite.internal.management.defragmentation;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand.DefragmentationStatusCommandArg;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTask;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTaskResult;

/** */
public class DefragmentationScheduleCommand implements Command<DefragmentationStatusCommandArg, VisorDefragmentationTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Schedule PDS defragmentation";
    }

    /** {@inheritDoc} */
    @Override public Class<DefragmentationScheduleCommandArg> argClass() {
        return DefragmentationScheduleCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorDefragmentationTask> taskClass() {
        return VisorDefragmentationTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        DefragmentationStatusCommandArg arg,
        VisorDefragmentationTaskResult res,
        Consumer<String> printer
    ) {
        printer.accept(res.getMessage());
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Collection<UUID> nodes, Predicate<UUID> isClient, DefragmentationStatusCommandArg arg0) {
        //TODO: implement filter node based on consistent id.
        return null;
    }
}
