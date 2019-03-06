/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Exception thrown during serialization if binary metadata isn't registered and it's registration isn't allowed.
 * Used for both binary types and marshalling mappings.
 * Confusing old class name is preserved for backwards compatibility.
 */
public class UnregisteredBinaryTypeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String MESSAGE =
        "Attempted to update binary metadata inside a critical synchronization block (will be " +
        "automatically retried). This exception must not be wrapped to any other exception class. " +
        "If you encounter this exception outside of EntryProcessor, please report to Apache Ignite " +
        "dev-list. Debug info [typeId=%d, binaryMetadata=%s, fut=%s]";

    /** */
    private static String createMessage(int typeId, BinaryMetadata binaryMetadata, GridFutureAdapter<?> fut) {
        return String.format(MESSAGE, typeId, binaryMetadata, fut);
    }

    /** */
    private final int typeId;

    /** */
    private final BinaryMetadata binaryMetadata;

    /** */
    private final GridFutureAdapter<?> fut;

    /**
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     */
    public UnregisteredBinaryTypeException(int typeId, BinaryMetadata binaryMetadata) {
        this(typeId, binaryMetadata, null);
    }

    /**
     * @param typeId Type ID.
     * @param fut Future to wait in handler.
     */
    public UnregisteredBinaryTypeException(int typeId, GridFutureAdapter<?> fut) {
        this(typeId, null, fut);
    }

    /**
     * @param typeId Type ID.
     * @param binaryMetadata Binary metadata.
     * @param fut Future to wait in handler.
     */
    private UnregisteredBinaryTypeException(int typeId, BinaryMetadata binaryMetadata, GridFutureAdapter<?> fut) {
        super(createMessage(typeId, binaryMetadata, fut));

        this.typeId = typeId;
        this.binaryMetadata = binaryMetadata;
        this.fut = fut;
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

    /**
     * @return Future to wait in handler.
     */
    public GridFutureAdapter<?> future() {
        return fut;
    }
}
