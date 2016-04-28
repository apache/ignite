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

export default ['ipaddress', ['IgniteInetAddress', (InetAddress) => {
    const onlyDigits = (str) => (/^\d+$/.test(str));

    const strictParseInt = (str) => onlyDigits(str) ? parseInt(str, 10) : Number.NaN;

    const parse = (commonIpAddress) => {
        const [ipOrHost, portRange] = commonIpAddress.split(':');
        const ports = _.isUndefined(portRange) ? [] : portRange.split('..').map(strictParseInt);

        return {ipOrHost, ports};
    };

    const link = (scope, el, attrs, [ngModel]) => {
        const isEmpty = (modelValue) => {
            return ngModel.$isEmpty(modelValue) || _.isUndefined(attrs.ipaddress) || attrs.ipaddress !== 'true';
        };

        if (attrs.ipaddressWithPort) {
            ngModel.$validators.ipaddressPort = (modelValue, viewValue) => {
                if (isEmpty(modelValue) || viewValue.indexOf(':') === -1)
                    return true;

                if ((viewValue.match(/:/g) || []).length > 1)
                    return false;

                const {ports} = parse(viewValue);

                if (ports.length !== 1)
                    return true;

                return InetAddress.validPort(ports[0]);
            };
        }

        if (attrs.ipaddressWithPortRange) {
            ngModel.$validators.ipaddressPortRange = (modelValue, viewValue) => {
                if (isEmpty(modelValue) || viewValue.indexOf('..') === -1)
                    return true;

                const {ports} = parse(viewValue);

                if (ports.length !== 2)
                    return ports.length < 2;

                return InetAddress.validPort(ports[0]) && InetAddress.validPort(ports[1]) && ports[0] < ports[1];
            };
        }

        ngModel.$validators.ipaddress = (modelValue, viewValue) => {
            if (isEmpty(modelValue))
                return true;

            const {ipOrHost, ports} = parse(viewValue);

            if (attrs.ipaddressWithPort || attrs.ipaddressWithPortRange || ports.length === 0)
                return InetAddress.validHost(ipOrHost);

            return false;
        };
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}]];
