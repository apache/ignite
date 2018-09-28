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

/**
 * @param {ReturnType<typeof import('app/services/InetAddress.service').default>} InetAddress
 */
export default function factory(InetAddress) {
    const onlyDigits = (str) => (/^\d+$/.test(str));

    const strictParseInt = (str) => onlyDigits(str) ? parseInt(str, 10) : Number.NaN;

    const parse = (commonIpAddress) => {
        const [ipOrHost, portRange] = commonIpAddress.split(':');
        const ports = _.isUndefined(portRange) ? [] : portRange.split('..').map(strictParseInt);

        return {ipOrHost, ports};
    };

    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, el, attrs, [ngModel]) => {
        const isEmpty = (modelValue) => {
            return ngModel.$isEmpty(modelValue) || _.isUndefined(attrs.ipaddress) || attrs.ipaddress !== 'true';
        };

        const portRange = !_.isNil(attrs.ipaddressWithPortRange);

        if (attrs.ipaddressWithPort) {
            ngModel.$validators.ipaddressPort = (modelValue) => {
                if (isEmpty(modelValue) || modelValue.indexOf(':') === -1)
                    return true;

                if ((modelValue.match(/:/g) || []).length > 1)
                    return false;

                const {ports} = parse(modelValue);

                if (ports.length !== 1)
                    return portRange;

                return InetAddress.validPort(ports[0]);
            };
        }

        if (portRange) {
            ngModel.$validators.ipaddressPortRange = (modelValue) => {
                if (isEmpty(modelValue) || modelValue.indexOf('..') === -1)
                    return true;

                const {ports} = parse(modelValue);

                if (ports.length !== 2)
                    return false;

                return InetAddress.validPort(ports[0]) && InetAddress.validPort(ports[1]) && ports[0] < ports[1];
            };
        }

        ngModel.$validators.ipaddress = (modelValue) => {
            if (isEmpty(modelValue))
                return true;

            const {ipOrHost, ports} = parse(modelValue);

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
}

factory.$inject = ['IgniteInetAddress'];
