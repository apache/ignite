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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataInfoTask;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataListResult;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataTypeArgs;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/** */
public class MetadataDetailsCommand
    extends MetadataAbstractSubCommand<MetadataTypeArgs, MetadataListResult>
{
    /** {@inheritDoc} */
    @Override protected String taskName() {
        return MetadataInfoTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public MetadataTypeArgs parseArguments0(CommandArgIterator argIter) {
        return MetadataTypeArgs.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override protected void printResult(MetadataListResult res, Logger log) {
        if (res.metadata() == null) {
            log.info("Type not found");

            return;
        }

        assert res.metadata().size() == 1 : "Unexpected  metadata results: " + res.metadata();

        BinaryMetadata m = F.first(res.metadata());

        log.info("typeId=" + printInt(m.typeId()));
        log.info("typeName=" + m.typeName());
        log.info("Fields:");

        final Map<Integer, String> fldMap = new HashMap<>();
        m.fieldsMap().forEach((name, fldMeta) -> {
            log.info(INDENT +
                "name=" + name +
                ", type=" + BinaryUtils.fieldTypeName(fldMeta.typeId()) +
                ", fieldId=" + printInt(fldMeta.fieldId()));

            fldMap.put(fldMeta.fieldId(), name);
        });

        log.info("Schemas:");

        m.schemas().forEach(s ->
            log.info(INDENT +
                "schemaId=" + printInt(s.schemaId()) +
                ", fields=" + Arrays.stream(s.fieldIds())
                    .mapToObj(fldMap::get)
                    .collect(Collectors.toList())));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.LIST.text();
    }
}
