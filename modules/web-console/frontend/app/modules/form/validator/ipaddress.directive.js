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
