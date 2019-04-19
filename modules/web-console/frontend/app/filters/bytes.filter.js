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

export default () => {
    /**
     * @param {number} bytes
     * @param {number} [precision]
     */
    const filter = (bytes, precision) => {
        if (bytes === 0)
            return '0 bytes';

        if (isNaN(parseFloat(bytes)) || !isFinite(bytes))
            return '-';

        if (typeof precision === 'undefined')
            precision = 1;

        const units = ['bytes', 'kB', 'MB', 'GB', 'TB'];
        const number = Math.floor(Math.log(bytes) / Math.log(1024));

        return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) + ' ' + units[number];
    };

    return filter;
};
