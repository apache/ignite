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

package org.apache.ignite.client;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A person entity for the tests.
 */
public class PersonBinarylizable implements Binarylizable {
    /** */
    private String name;

    /** */
    private boolean writeThrows;

    /** */
    private boolean readThrows;

    /** */
    private boolean readWaits;

    /** */
    public PersonBinarylizable(String name) {
        this(name, false, false, false);
    }

    /** */
    public PersonBinarylizable(String name, boolean writeThrows, boolean readThrows, boolean readWaits) {
        this.name = name;
        this.writeThrows = writeThrows;
        this.readThrows = readThrows;
        this.readWaits = readWaits;
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public void setName(String name) {
        this.name = name;
    }

    /** */
    public boolean isWriteThrows() {
        return writeThrows;
    }

    /** */
    public void setWriteThrows(boolean writeThrows) {
        this.writeThrows = writeThrows;
    }

    /** */
    public boolean isReadThrows() {
        return readThrows;
    }

    /** */
    public void setReadThrows(boolean readThrows) {
        this.readThrows = readThrows;
    }

    /** */
    public boolean isReadWaits() {
        return readWaits;
    }

    /** */
    public void setReadWaits(boolean readWaits) {
        this.readWaits = readWaits;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("name", name);
        writer.writeBoolean("writeThrows", writeThrows);
        writer.writeBoolean("readThrows", readThrows);
        writer.writeBoolean("readWaits", readWaits);

        if (writeThrows)
            throw new ArithmeticException("_write_");
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        name = reader.readString("name");
        writeThrows = reader.readBoolean("writeThrows");
        readThrows = reader.readBoolean("readThrows");
        readWaits = reader.readBoolean("readWaits");

        if (readWaits) {
            try {
                U.sleep(1000);
            }
            catch (IgniteInterruptedCheckedException e) {
                e.printStackTrace();

                Thread.currentThread().interrupt();
            }
        }

        if (readThrows)
            throw new ArithmeticException("_read_");
    }
}
