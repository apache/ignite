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

import isEmpty from 'lodash/isEmpty';
import {nonEmpty} from 'app/utils/lodashMixins';

export default class {
    static $inject = ['JavaTypes'];

    /**
     * @param JavaTypes service.
     */
    constructor(JavaTypes) {
        this.JavaTypes = JavaTypes;
    }

    extractMessage(err, prefix) {
        prefix = prefix || '';

        if (err) {
            if (err.hasOwnProperty('data'))
                err = err.data;

            if (err.hasOwnProperty('message')) {
                let msg = err.message;

                const traceIndex = msg.indexOf(', trace=');

                if (traceIndex > 0)
                    msg = msg.substring(0, traceIndex);

                const lastIdx = msg.lastIndexOf(' err=');
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

                return prefix + (lastIdx >= 0 ? msg.substring(lastIdx + 5, msgEndIdx > 0 ? msgEndIdx : traceIndex) : msg);
            }

            if (nonEmpty(err.className)) {
                if (isEmpty(prefix))
                    prefix = 'Internal cluster error: ';

                return prefix + err.className;
            }

            return prefix + err;
        }

        return prefix + 'Internal error.';
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
