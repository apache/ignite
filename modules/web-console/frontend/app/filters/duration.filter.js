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

export default () => {
    /**
     * @param {number} t Time in ms.
     */
    const filter = (t) => {
        if (t === 9223372036854775807)
            return 'Infinite';

        const a = (i, suffix) => i && i !== '00' ? i + suffix + ' ' : '';

        const cd = 24 * 60 * 60 * 1000;
        const ch = 60 * 60 * 1000;
        const cm = 60 * 1000;
        const cs = 1000;

        const d = Math.floor(t / cd);
        const h = Math.floor((t - d * cd) / ch);
        const m = Math.floor((t - d * cd - h * ch) / cm);
        const s = Math.floor((t - d * cd - h * ch - m * cm) / cs);
        const ms = Math.round(t % 1000);

        return a(d, 'd') + a(h, 'h') + a(m, 'm') + a(s, 's') + (t < 1000 || (t < cm && ms !== 0) ? ms + 'ms' : '');
    };

    return filter;
};
