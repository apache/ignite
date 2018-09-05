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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown during serialization if binary metadata isn't registered and it's registration isn't allowed.
 */
public class UnregisteredBinaryTypeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final int typeId;

    /** */
    private final BinaryMetadata binaryMetadata;

    /**
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     */
    public UnregisteredBinaryTypeException(int typeId, BinaryMetadata binaryMetadata) {
        this.typeId = typeId;
        this.binaryMetadata = binaryMetadata;
    }

    /**
     * @param msg Error message.
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     */
    public UnregisteredBinaryTypeException(String msg, int typeId,
        BinaryMetadata binaryMetadata) {
        super(msg);
        this.typeId = typeId;
        this.binaryMetadata = binaryMetadata;
    }

    /**
     * @param cause Non-null throwable cause.
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     */
    public UnregisteredBinaryTypeException(Throwable cause, int typeId,
        BinaryMetadata binaryMetadata) {
        super(cause);
        this.typeId = typeId;
        this.binaryMetadata = binaryMetadata;
    }

    /**
     * @param msg Error message.
     * @param cause Non-null throwable cause.
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     */
    public UnregisteredBinaryTypeException(String msg, @Nullable Throwable cause, int typeId,
        BinaryMetadata binaryMetadata) {
        super(msg, cause);
        this.typeId = typeId;
        this.binaryMetadata = binaryMetadata;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Binary metadata.
     */
    public BinaryMetadata binaryMetadata() {
        return binaryMetadata;
    }
}
