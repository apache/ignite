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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataMarshalled;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataUpdateTask;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class MetadataUpdateCommand
    extends MetadataAbstractSubCommand<MetadataMarshalled, MetadataMarshalled>
{
    /** Output file name. */
    public static final String IN_FILE_NAME = "--in";

    /** {@inheritDoc} */
    @Override protected String taskName() {
        return MetadataUpdateTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will update the binary metadata at the cluster.";
    }

    /** {@inheritDoc} */
    @Override public MetadataMarshalled parseArguments0(CommandArgIterator argIter) {
        String opt = argIter.nextArg(IN_FILE_NAME);

        if (!IN_FILE_NAME.equalsIgnoreCase(opt))
            throw new IllegalArgumentException("The option '" + IN_FILE_NAME + "' is required");

        Path inFile = FS.getPath(argIter.nextArg("input file name"));

        try (InputStream is = Files.newInputStream(inFile)) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();

            U.copy(is, buf);

            return new MetadataMarshalled(buf.toByteArray(), null);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Cannot read metadata from " + inFile, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void printResult(MetadataMarshalled res, Logger log) {
        if (res.metadata() == null) {
            log.info("Type not found");

            return;
        }

        BinaryMetadata m = res.metadata();

        log.info("Metadata updated for the type: '" + m.typeName() + '\'');
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.UPDATE.text();
    }
}
