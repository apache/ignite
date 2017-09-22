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

import ProgressPlugin from 'webpack/lib/ProgressPlugin';

let chars = 0;
let lastState = 0;
let lastStateTime = 0;

const outputStream = process.stdout;

const _goToLineStart = (nextMessage) => {
    let str = '';

    for (; chars > nextMessage.length; chars--)
        str += '\b \b';

    chars = nextMessage.length;

    for (let i = 0; i < chars; i++)
        str += '\b';

    if (str)
        outputStream.write(str);
};

export default new ProgressPlugin((percentage, msg) => {
    let state = msg;

    if (percentage < 1) {
        percentage = Math.floor(percentage * 100);

        msg = percentage + '% ' + msg;

        if (percentage < 100)
            msg = ' ' + msg;

        if (percentage < 10)
            msg = ' ' + msg;
    }

    state = state.replace(/^\d+\/\d+\s+/, '');

    if (percentage === 0) {
        lastState = null;
        lastStateTime = (new Date()).getTime();
    }
    else if (state !== lastState || percentage === 1) {
        const now = (new Date()).getTime();

        if (lastState) {
            const stateMsg = (now - lastStateTime) + 'ms ' + lastState;

            _goToLineStart(stateMsg);

            outputStream.write(stateMsg + '\n');

            chars = 0;
        }

        lastState = state;
        lastStateTime = now;
    }

    _goToLineStart(msg);

    outputStream.write(msg);
});
