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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataMarshalled;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataRemoveTask;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.lang.IgniteExperimental;

/** */
@IgniteExperimental
public class MetaRemoveCommand implements ComputeCommand<MetaRemoveCommandArg, MetadataMarshalled> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Remove the metadata of the specified type " +
            "(the type must be specified by type name or by type identifier) " +
            "from cluster and saves the removed metadata to the specified file.\n" +
            "If the file name isn't specified the output file name is: '<typeId>.bin'";
    }

    /** {@inheritDoc} */
    @Override public Class<MetaRemoveCommandArg> argClass() {
        return MetaRemoveCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<MetadataRemoveTask> taskClass() {
        return MetadataRemoveTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(MetaRemoveCommandArg arg, MetadataMarshalled res, Consumer<String> printer) {
        if (res.metadata() == null)
            throw new IllegalArgumentException("Failed to remove binary type, type not found: " + arg);

        BinaryMetadata m = res.metadata();

        Path outFile = FileSystems.getDefault().getPath(arg.out() != null
            ? arg.out()
            : BinaryUtils.binaryMetaFileName(m.typeId())
        );

        try (OutputStream os = Files.newOutputStream(outFile)) {
            os.write(res.metadataMarshalled());
        }
        catch (IOException e) {
            printer.accept("Cannot store removed type '" + m.typeName() + "' to: " + outFile);
            printer.accept(e.getMessage());

            return;
        }

        printer.accept("Type '" + m.typeName() + "' is removed. Metadata is stored at: " + outFile);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(MetaRemoveCommandArg arg) {
        return "Warning: the command will remove the binary metadata for a type \""
            + (arg.typeId() != 0 ? arg.typeId() : arg.typeName()) + "\" from cluster.";
    }
}
