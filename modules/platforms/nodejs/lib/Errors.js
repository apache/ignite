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

'use strict';

const Util = require('util');

/**
 * Base Ignite client error class.
 */
class IgniteClientError extends Error {
    constructor(message) {
        super(message);
    }

    /**
     * Ignite client does not support one of the specified or received data types.
     * @ignore
     */
    static unsupportedTypeError(type) {
        const BinaryUtils = require('./internal/BinaryUtils');
        return new IgniteClientError(Util.format('Type %s is not supported', BinaryUtils.getTypeName(type)));
    }

    /**
     * The real type of data is not equal to the specified one.
     * @ignore
     */
    static typeCastError(fromType, toType) {
        const BinaryUtils = require('./internal/BinaryUtils');
        return new IgniteClientError(Util.format('Type "%s" can not be cast to %s',
            BinaryUtils.getTypeName(fromType), BinaryUtils.getTypeName(toType)));
    }

    /**
     * The real value can not be cast to the specified type.
     * @ignore
     */
    static valueCastError(value, toType) {
        const BinaryUtils = require('./internal/BinaryUtils');
        return new IgniteClientError(Util.format('Value "%s" can not be cast to %s',
            value, BinaryUtils.getTypeName(toType)));
    }

    /**
     * An illegal or inappropriate argument has been passed to the API method.
     * @ignore
     */
    static illegalArgumentError(message) {
        return new IgniteClientError(message);
    }

    /**
     * Ignite client internal error.
     * @ignore
     */
    static internalError(message = null) {
        return new IgniteClientError(message || 'Internal library error');
    }

    /**
     * Serialization/deserialization errors.
     * @ignore
     */
    static serializationError(serialize, message = null) {
        let msg = serialize ? 'Complex object can not be serialized' : 'Complex object can not be deserialized';
        if (message) {
            msg = msg + ': ' + message;
        }
        return new IgniteClientError(msg);
    }

    /**
     * EnumItem serialization/deserialization errors.
     * @ignore
     */
    static enumSerializationError(serialize, message = null) {
        let msg = serialize ? 'Enum item can not be serialized' : 'Enum item can not be deserialized';
        if (message) {
            msg = msg + ': ' + message;
        }
        return new IgniteClientError(msg);
    }
}

/**
 * Ignite server returns error for the requested operation.
 * @extends IgniteClientError
 */
class OperationError extends IgniteClientError {
    constructor(message) {
        super(message);
    }
}

/**
 * Ignite client is not in an appropriate state for the requested operation.
 * @extends IgniteClientError
 */
class IllegalStateError extends IgniteClientError {
    constructor(message = null) {
        super(message || 'Ignite client is not in an appropriate state for the requested operation');
    }
}

/**
 * The requested operation is not completed due to the connection lost.
 * @extends IgniteClientError
 */
class LostConnectionError extends IgniteClientError {
    constructor(message = null) {
        super(message || 'Request is not completed due to the connection lost');
    }
}

module.exports.IgniteClientError = IgniteClientError;
module.exports.OperationError = OperationError;
module.exports.IllegalStateError = IllegalStateError;
module.exports.LostConnectionError = LostConnectionError;
