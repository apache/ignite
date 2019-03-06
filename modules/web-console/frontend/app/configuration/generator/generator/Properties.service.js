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

import StringBuilder from './StringBuilder';

/**
 * Properties generation entry point.
 */
export default class IgnitePropertiesGenerator {
    _collectProperties(bean) {
        const props = [];

        // Append properties for complex object.
        const processBean = (bean) => {
            const newProps = _.difference(this._collectProperties(bean), props);

            if (!_.isEmpty(newProps)) {
                props.push(...newProps);

                if (!_.isEmpty(_.last(props)))
                    props.push('');
            }
        };

        // Append properties from item.
        const processItem = (item) => {
            switch (item.clsName) {
                case 'PROPERTY':
                case 'PROPERTY_CHAR':
                case 'PROPERTY_INT':
                    props.push(..._.difference([`${item.value}=${item.hint}`], props));

                    break;
                case 'BEAN':
                case 'DATA_SOURCE':
                    processBean(item.value);

                    break;
                case 'ARRAY':
                case 'COLLECTION':
                    _.forEach(item.items, processBean);

                    break;
                case 'MAP':
                    // Generate properties for all objects in keys and values of map.
                    _.forEach(item.entries, (entry) => {
                        processBean(entry.name);
                        processBean(entry.value);
                    });

                    break;
                default:
                    // No-op.
            }
        };

        // Generate properties for object arguments.
        _.forEach(_.get(bean, 'arguments'), processItem);

        // Generate properties for object properties.
        _.forEach(_.get(bean, 'properties'), processItem);

        return props;
    }

    generate(cfg) {
        const lines = this._collectProperties(cfg);

        if (_.isEmpty(lines))
            return null;

        const sb = new StringBuilder();

        sb.append(`# ${sb.generatedBy()}`).emptyLine();

        _.forEach(lines, (line) => sb.append(line));

        return sb.asString();
    }
}
