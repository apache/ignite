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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class MetaUpdateCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(example = "<fileName>")
    private String in;

    /** Marshaled metadata. */
    private byte[] metaMarshalled;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, in);
        U.writeByteArray(out, metaMarshalled);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        this.in = U.readString(in);
        metaMarshalled = U.readByteArray(in);
    }

    /** */
    public byte[] metaMarshalled() {
        return metaMarshalled;
    }

    /** */
    public void metaMarshalled(byte[] metaMarshalled) {
        this.metaMarshalled = metaMarshalled;
    }

    /** */
    public String in() {
        return in;
    }

    /** */
    public void in(String in) {
        Path inFile = FileSystems.getDefault().getPath(in);

        try (InputStream is = Files.newInputStream(inFile)) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();

            U.copy(is, buf);

            metaMarshalled = buf.toByteArray();
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Cannot read metadata from " + inFile, e);
        }
        this.in = in;
    }
}
