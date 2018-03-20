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

'use strict';

const Util = require('util');

/**
 * Base Ignite client error class.
 */
class IgniteClientError extends Error {
    constructor(message) {
        super(message);
    }
}

/**
 * Ignite server returns error for the requested operation.
 */
class OperationError extends IgniteClientError {
    constructor(message) {
        super(message);
    }
}

/**
 * Ignite client is not in an appropriate state for the requested operation.
 */
class IllegalStateError extends IgniteClientError {
    constructor(message = null) {
        super(message || 'The client is disconnected');
    }
}

/**
 * Ignite client does not support one of the specified or received data types.
 */
class UnsupportedTypeError extends IgniteClientError {
    constructor(type) {
        const BinaryUtils = require('./internal/BinaryUtils');
        super(Util.format('Type %s is not supported', BinaryUtils.getTypeName(type)));
    }
}

/**
 * The real type of data is not equal to the specified one.
 */
class TypeCastError extends IgniteClientError {
    constructor(fromType, toType) {
        const BinaryUtils = require('./internal/BinaryUtils');
        super(Util.format('Type %s can not be cast to %s',
            BinaryUtils.getTypeName(fromType), BinaryUtils.getTypeName(toType)));
    }
}

/**
 * An illegal or inappropriate argument has been passed to the API method.
 */
class IllegalArgumentError extends IgniteClientError {
    constructor(message) {
        super(message);
    }
}

/**
 * The requested operation is not completed due to the connection lost.
 */
class LostConnectionError extends IgniteClientError {
    constructor(message = null) {
        super(message || 'Request is not completed due to the connection lost');
    }
}

/**
 * Ignite client internal error.
 */
class InternalError extends IgniteClientError {
    constructor(message = null) {
        super(message || 'Internal library error');
    }
}

module.exports.IgniteClientError = IgniteClientError;
module.exports.OperationError = OperationError;
module.exports.IllegalStateError = IllegalStateError;
module.exports.UnsupportedTypeError = UnsupportedTypeError;
module.exports.TypeCastError = TypeCastError;
module.exports.IllegalArgumentError = IllegalArgumentError;
module.exports.LostConnectionError = LostConnectionError;
module.exports.InternalError = InternalError;
