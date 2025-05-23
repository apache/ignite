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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.util.typedef.internal.U;
import static org.apache.ignite.internal.management.meta.MetaListCommand.printInt;

/** */
@ArgumentGroup(value = {"typeId", "typeName"}, onlyOneOf = true, optional = false)
public class MetaDetailsCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true, example = "<typeId>", javaStyleName = true)
    private int typeId;

    /** */
    @Argument(optional = true, example = "<typeName>", javaStyleName = true)
    private String typeName;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(typeId);
        U.writeString(out, typeName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        typeId = in.readInt();
        typeName = U.readString(in);
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /** */
    public String typeName() {
        return typeName;
    }

    /** */
    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return typeId != 0 ? printInt(typeId) : typeName;
    }
}
