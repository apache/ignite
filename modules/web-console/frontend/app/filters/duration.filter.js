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

export default () => {
    /**
     * @param {number} t Time in ms.
     * @param {string} dflt Default value.
     */
    const filter = (t, dflt = '0') => {
        if (t === 9223372036854775807)
            return 'Infinite';

        if (t <= 0)
            return dflt;

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
