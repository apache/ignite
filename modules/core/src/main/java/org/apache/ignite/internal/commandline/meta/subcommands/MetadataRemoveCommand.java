/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.meta.subcommands;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataMarshalled;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataRemoveTask;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataTypeArgs;

/**
 *
 */
public class MetadataRemoveCommand
    extends MetadataAbstractSubCommand<MetadataTypeArgs, MetadataMarshalled> {
    /** Output file name. */
    public static final String OUT_FILE_NAME = "--out";

    /** Output file. */
    private Path outFile;

    /** {@inheritDoc} */
    @Override protected String taskName() {
        return MetadataRemoveTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will remove the binary metadata for a type \""
            + arg().toString() + "\" from cluster.";
    }

    /** {@inheritDoc} */
    @Override public MetadataTypeArgs parseArguments0(CommandArgIterator argIter) {
        outFile = null;

        MetadataTypeArgs argType = MetadataTypeArgs.parseArguments(argIter);

        while (argIter.hasNextSubArg() && outFile == null) {
            String opt = argIter.nextArg("");

            if (OUT_FILE_NAME.equalsIgnoreCase(opt)) {
                String fileName = argIter.nextArg("output file name");

                outFile = FS.getPath(fileName);
            }
        }

        if (outFile != null) {
            try (OutputStream os = Files.newOutputStream(outFile)) {
                os.close();

                Files.delete(outFile);
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Cannot write to output file " + outFile +
                    ". Error: " + e.toString(), e);
            }
        }

        return argType;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(MetadataMarshalled res, Logger log) {
        if (res.metadata() == null) {
            log.info("Type not found");

            return;
        }

        BinaryMetadata m = res.metadata();

        if (outFile == null)
            outFile = FS.getPath(m.typeId() + ".bin");

        try (OutputStream os = Files.newOutputStream(outFile)) {
            os.write(res.metadataMarshalled());
        }
        catch (IOException e) {
            log.severe("Cannot store removed type '" + m.typeName() + "' to: " + outFile);
            log.severe(CommandLogger.errorMessage(e));

            return;
        }

        log.info("Type '" + m.typeName() + "' is removed. Metadata is stored at: " + outFile);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.REMOVE.text();
    }
}
