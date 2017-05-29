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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener response.
 */
public class SqlListenerResponse implements RawBinarylizable {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    private int status;

    /** Error. */
    private String err;

    /** Response object. */
    @GridToStringInclude
    private RawBinarylizable res;

    /**
     * Constructs is used for deserialization.
     *
     * @param resClass Response result.
     * @throws IllegalAccessException On error.
     * @throws InstantiationException On error.
     */
    public SqlListenerResponse(Class<? extends RawBinarylizable> resClass)
        throws IllegalAccessException, InstantiationException {
        res = resClass.newInstance();
    }

    /**
     * Constructs successful rest response.
     *
     * @param res Response result.
     */
    public SqlListenerResponse(RawBinarylizable res) {
        status = STATUS_SUCCESS;

        this.res = res;
        err = null;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public SqlListenerResponse(int status, @Nullable String err) {
        assert status != STATUS_SUCCESS;

        this.status = status;

        res = null;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int status() {
        return status;
    }

    /**
     * @return Response object.
     */
    public <R extends RawBinarylizable> R response() {
        return (R)res;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlListenerResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        writer.writeByte((byte) status);

        if (status != STATUS_SUCCESS)
            writer.writeString(err);
        else if (res != null)
            res.writeBinary(writer, objWriter);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        status = reader.readByte();

        if (status != STATUS_SUCCESS) {
            err = reader.readString();
            res = null;
        }
        else
            res.readBinary(reader, objReader);
    }
}
