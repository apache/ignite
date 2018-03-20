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
const Errors = require('../Errors');

/** Helper class for the library methods arguments check. */
class ArgumentChecker {
    static notEmpty(arg, argName) {
        if (!arg || arg instanceof Array && arg.length === 0) {
            throw new Errors.IllegalArgumentError(Util.format('"%s" argument should not be empty', argName));
        }
    }

    static notNull(arg, argName) {
        if (arg === null || arg === undefined) {
            throw new Errors.IllegalArgumentError(Util.format('"%s" argument should not be null', argName));
        }
    }

    static hasType(arg, argName, ...types) {
        if (arg === null) {
            return;
        }
        for (let type of types) {
            if (typeof type !== 'string' && arg instanceof type || typeof arg === type) {
                return;
            }
        }
        throw new Errors.IllegalArgumentError(Util.format('"%s" argument has incorrect type', argName));
    }

    static hasValueFrom(arg, argName, values) {
        if (!Object.values(values).includes(arg)) {
            throw new Errors.IllegalArgumentError(Util.format('"%s" argument has incorrect value', argName));
        }
    }
}

module.exports = ArgumentChecker;
