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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataInfoTask;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataListResult;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.meta.MetaListCommand.printInt;

/** */
@IgniteExperimental
public class MetaDetailsCommand implements ComputeCommand<IgniteDataTransferObject, MetadataListResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print detailed info about specified binary type " +
            "(the type must be specified by type name or by type identifier)";
    }

    /** {@inheritDoc} */
    @Override public Class<MetaDetailsCommandArg> argClass() {
        return MetaDetailsCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<MetadataInfoTask> taskClass() {
        return MetadataInfoTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(IgniteDataTransferObject arg, MetadataListResult res, Consumer<String> printer) {
        if (res.metadata() == null) {
            printer.accept("Type not found");

            return;
        }

        assert res.metadata().size() == 1 : "Unexpected  metadata results: " + res.metadata();

        BinaryMetadata m = F.first(res.metadata());

        printer.accept("typeId=" + printInt(m.typeId()));
        printer.accept("typeName=" + m.typeName());
        printer.accept("Fields:");

        final Map<Integer, String> fldMap = new HashMap<>();
        m.fieldsMap().forEach((name, fldMeta) -> {
            printer.accept(INDENT +
                "name=" + name +
                ", type=" + BinaryUtils.fieldTypeName(fldMeta.typeId()) +
                ", fieldId=" + printInt(fldMeta.fieldId()));

            fldMap.put(fldMeta.fieldId(), name);
        });

        printer.accept("Schemas:");

        m.schemas().forEach(s ->
            printer.accept(INDENT +
                "schemaId=" + printInt(s.schemaId()) +
                ", fields=" + Arrays.stream(s.fieldIds())
                .mapToObj(fldMap::get)
                .collect(Collectors.toList())));
    }
}
