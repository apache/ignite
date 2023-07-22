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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.lang.IgniteExperimental;

/** */
@IgniteExperimental
public class MetaListCommand implements ComputeCommand<IgniteDataTransferObject, MetadataListResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print list of binary metadata types";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<MetadataInfoTask> taskClass() {
        return MetadataInfoTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(IgniteDataTransferObject arg, MetadataListResult res, Consumer<String> printer) {
        for (BinaryMetadata m : res.metadata()) {
            printer.accept("typeId=" + printInt(m.typeId()) +
                ", typeName=" + m.typeName() +
                ", fields=" + m.fields().size() +
                ", schemas=" + m.schemas().size() +
                ", isEnum=" + m.isEnum());
        }
    }

    /**
     * @param val Integer value.
     * @return String.
     */
    public static String printInt(int val) {
        return "0x" + Integer.toHexString(val).toUpperCase() + " (" + val + ')';
    }
}
