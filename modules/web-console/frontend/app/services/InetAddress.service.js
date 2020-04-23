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

import _ from 'lodash';

export default function() {
    return {
        /**
         * @param {String} ip IP address to check.
         * @returns {boolean} 'true' if given ip address is valid.
         */
        validIp(ip) {
            const regexp = /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/;

            return regexp.test(ip);
        },
        /**
         * @param {String} hostNameOrIp host name or ip address to check.
         * @returns {boolean} 'true' if given is host name or ip.
         */
        validHost(hostNameOrIp) {
            const regexp = /^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$/;

            return regexp.test(hostNameOrIp) || this.validIp(hostNameOrIp);
        },
        /**
         * @param {number} port Port value to check.
         * @returns boolean 'true' if given port is valid tcp/udp port range.
         */
        validPort(port) {
            return _.isInteger(port) && port > 0 && port <= 65535;
        },
        /**
         * @param {number} port Port value to check.
         * @returns {boolean} 'true' if given port in non system port range(user+dynamic).
         */
        validNonSystemPort(port) {
            return _.isInteger(port) && port >= 1024 && port <= 65535;
        }
    };
}
