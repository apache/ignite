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

package org.apache.ignite.internal.management.meta;

import java.util.function.Consumer;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataMarshalled;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataUpdateTask;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.jetbrains.annotations.Nullable;

/** */
public class MetaUpdateCommand implements
    ExperimentalCommand<MetaUpdateCommandArg, MetadataMarshalled>,
    ComputeCommand<MetaUpdateCommandArg, MetadataMarshalled> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Update cluster metadata from specified file (file name is required)";
    }

    /** {@inheritDoc} */
    @Override public Class<MetaUpdateCommandArg> argClass() {
        return MetaUpdateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<MetadataUpdateTask> taskClass() {
        return MetadataUpdateTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(MetaUpdateCommandArg arg, MetadataMarshalled res, Consumer<String> printer) {
        if (res.metadata() == null) {
            printer.accept("Type not found");

            return;
        }

        BinaryMetadata m = res.metadata();

        printer.accept("Metadata updated for the type: '" + m.typeName() + '\'');
    }

    /** {@inheritDoc} */
    @Override public @Nullable String confirmationPrompt(GridClient cli, MetaUpdateCommandArg arg) {
        return "Warning: the command will update the binary metadata at the cluster.";
    }
}
