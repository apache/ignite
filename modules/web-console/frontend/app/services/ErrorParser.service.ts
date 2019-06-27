/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import isEmpty from 'lodash/isEmpty';
import {nonEmpty} from 'app/utils/lodashMixins';

const CAUSE_STR = 'Caused by: ';
const ERR_START_STR = ' err=';

export default class {
    static $inject = ['JavaTypes'];

    /**
     * @param {import('./JavaTypes.service').default} JavaTypes
     */
    constructor(JavaTypes) {
        this.JavaTypes = JavaTypes;
    }

    parse(err, prefix) {
        prefix = prefix || '';

        if (err) {
            if (err.hasOwnProperty('data'))
                err = err.data;

            if (err.hasOwnProperty('message')) {
                let msg = err.message;

                const traceIndex = msg.indexOf(', trace=');

                if (traceIndex > 0)
                    msg = msg.substring(0, traceIndex);

                const lastIdx = msg.lastIndexOf(ERR_START_STR);
                let msgEndIdx = msg.indexOf(']', lastIdx);

                if (lastIdx > 0 && msgEndIdx > 0) {
                    let startIdx = msg.indexOf('[', lastIdx);

                    while (startIdx > 0) {
                        const tmpIdx = msg.indexOf(']', msgEndIdx + 1);

                        if (tmpIdx > 0)
                            msgEndIdx = tmpIdx;

                        startIdx = msg.indexOf('[', startIdx + 1);
                    }
                }

                const causes = [];

                let causeIdx = err.message.indexOf(CAUSE_STR);

                while (causeIdx >= 0) {
                    // Find next ": " in cause message to skip exception class name.
                    const msgStart = err.message.indexOf(': ', causeIdx + CAUSE_STR.length) + 2;
                    const causeEndLine = err.message.indexOf('\n', msgStart);
                    const msgEnd = err.message.indexOf('[', msgStart);
                    const cause = err.message.substring(msgStart, msgEnd >= 0 && msgEnd < causeEndLine ? msgEnd : causeEndLine);

                    causes.unshift(cause);

                    causeIdx = err.message.indexOf(CAUSE_STR, causeIdx + CAUSE_STR.length);
                }

                return new ErrorParseResult(
                    prefix + (lastIdx >= 0
                        ? msg.substring(lastIdx + ERR_START_STR.length, msgEndIdx > 0 ? msgEndIdx : traceIndex)
                        : msg),
                    causes
                );
            }

            if (nonEmpty(err.className)) {
                if (isEmpty(prefix))
                    prefix = 'Internal cluster error: ';

                return new ErrorParseResult(prefix + err.className);
            }

            return new ErrorParseResult(prefix + err);
        }

        return new ErrorParseResult(prefix + 'Internal error.');
    }

    extractFullMessage(err) {
        const clsName = _.isEmpty(err.className) ? '' : '[' + this.JavaTypes.shortClassName(err.className) + '] ';

        let msg = err.message || '';
        const traceIndex = msg.indexOf(', trace=');

        if (traceIndex > 0)
            msg = msg.substring(0, traceIndex);

        return clsName + (msg);
    }
}

/**
 * Information with error parsing result.
 */
export class ErrorParseResult {
    /** String with parsed error message. */
    message: String;

    /** List of error causes in reverse order. */
    causes: String[];

    /**
     * @param {String} message String with parsed error message.
     * @param {Array.<String>} causes List of error causes in reverse order.
     */
    constructor(message: String, causes = []) {
        this.message = message;
        this.causes = causes;
    }
}
