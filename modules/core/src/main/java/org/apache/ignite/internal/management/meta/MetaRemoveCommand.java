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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import lombok.Data;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
@Data
public class MetaRemoveCommand extends BaseCommand implements ExperimentalCommand {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true, javaStyleExample = true, javaStyleName = true, brackets = true)
    private long typeId;

    /** */
    @Argument(optional = true, javaStyleExample = true, javaStyleName = true, brackets = true)
    private String typeName;

    /** */
    @Argument(optional = true, javaStyleExample = true, javaStyleName = true, example = "<fileName>")
    private String out;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Remove the metadata of the specified type " +
            "(the type must be specified by type name or by type identifier) " +
            "from cluster and saves the removed metadata to the specified file.\n" +
            "If the file name isn't specified the output file name is: '<typeId>.bin'";
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        out.writeLong(typeId);
        U.writeString(out, typeName);
        U.writeString(out, this.out);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        typeId = in.readLong();
        typeName = U.readString(in);
        out = U.readString(in);
    }
}
