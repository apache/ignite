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
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"typeId", "typeName"}, onlyOneOf = true, optional = false)
public class MetaRemoveCommandArg extends MetaDetailsCommandArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true, example = "<fileName>")
    private String out;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeString(out, this.out);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        out = U.readString(in);
    }

    /** */
    public String out() {
        return out;
    }

    /** */
    public void out(String out) {
        Path outFile = FileSystems.getDefault().getPath(out);

        try (OutputStream os = Files.newOutputStream(outFile)) {
            os.close();

            Files.delete(outFile);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Cannot write to output file " + outFile +
                ". Error: " + e.toString(), e);
        }

        this.out = out;
    }
}
