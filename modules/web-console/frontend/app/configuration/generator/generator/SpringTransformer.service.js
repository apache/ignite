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

import AbstractTransformer from './AbstractTransformer';
import StringBuilder from './StringBuilder';
import VersionService from 'app/services/Version.service';

const versionService = new VersionService();

export default class IgniteSpringTransformer extends AbstractTransformer {
    static escapeXml(str = '') {
        return str.replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;')
            .replace(/>/g, '&gt;')
            .replace(/</g, '&lt;');
    }

    static commentBlock(sb, ...lines) {
        if (lines.length > 1) {
            sb.append('<!--');

            _.forEach(lines, (line) => sb.append(`  ${line}`));

            sb.append('-->');
        }
        else
            sb.append(`<!-- ${_.head(lines)} -->`);
    }

    static appendBean(sb, bean, appendId) {
        const beanTags = [];

        if (appendId)
            beanTags.push(`id="${bean.id}"`);

        beanTags.push(`class="${bean.clsName}"`);

        if (bean.factoryMtd)
            beanTags.push(`factory-method="${bean.factoryMtd}"`);

        sb.startBlock(`<bean ${beanTags.join(' ')}>`);

        _.forEach(bean.arguments, (arg) => {
            if (arg.clsName === 'MAP') {
                sb.startBlock('<constructor-arg>');
                this._constructMap(sb, arg);
                sb.endBlock('</constructor-arg>');
            }
            else if (_.isNil(arg.value)) {
                sb.startBlock('<constructor-arg>');
                sb.append('<null/>');
                sb.endBlock('</constructor-arg>');
            }
            else if (arg.constant) {
                sb.startBlock('<constructor-arg>');
                sb.append(`<util:constant static-field="${arg.clsName}.${arg.value}"/>`);
                sb.endBlock('</constructor-arg>');
            }
            else if (arg.clsName === 'BEAN') {
                sb.startBlock('<constructor-arg>');
                this.appendBean(sb, arg.value);
                sb.endBlock('</constructor-arg>');
            }
            else
                sb.append(`<constructor-arg value="${this._toObject(arg.clsName, arg.value)}"/>`);
        });

        this._setProperties(sb, bean);

        sb.endBlock('</bean>');
    }

    static _toObject(clsName, items) {
        return _.map(_.isArray(items) ? items : [items], (item) => {
            switch (clsName) {
                case 'PROPERTY':
                case 'PROPERTY_CHAR':
                case 'PROPERTY_INT':
                    return `\${${item}}`;
                case 'java.lang.Class':
                    return this.javaTypes.fullClassName(item);
                case 'long':
                    return `${item}`;
                case 'java.lang.String':
                case 'PATH':
                    return this.escapeXml(item);
                default:
                    return item;
            }
        });
    }

    static _isBean(clsName) {
        return this.javaTypes.nonBuiltInClass(clsName) && this.javaTypesNonEnum.nonEnum(clsName) && _.includes(clsName, '.');
    }

    static _setCollection(sb, prop) {
        sb.startBlock(`<property name="${prop.name}">`);
        sb.startBlock('<list>');

        _.forEach(prop.items, (item, idx) => {
            if (this._isBean(prop.typeClsName)) {
                if (idx !== 0)
                    sb.emptyLine();

                this.appendBean(sb, item);
            }
            else
                sb.append(`<value>${item}</value>`);
        });

        sb.endBlock('</list>');
        sb.endBlock('</property>');
    }

    static _constructMap(sb, map) {
        sb.startBlock('<map>');

        _.forEach(map.entries, (entry) => {
            const key = entry[map.keyField];
            const val = entry[map.valField];

            const isKeyBean = this._isBean(map.keyClsName);
            const isValBean = this._isBean(map.valClsName);


            if (isKeyBean || isValBean) {
                sb.startBlock('<entry>');

                sb.startBlock('<key>');
                if (isKeyBean)
                    this.appendBean(sb, key);
                else
                    sb.append(this._toObject(map.keyClsName, key));
                sb.endBlock('</key>');

                sb.startBlock('<value>');
                if (isValBean)
                    this.appendBean(sb, val);
                else
                    sb.append(this._toObject(map.valClsName, val));
                sb.endBlock('</value>');

                sb.endBlock('</entry>');
            }
            else
                sb.append(`<entry key="${this._toObject(map.keyClsName, key)}" value="${this._toObject(map.valClsName, val)}"/>`);
        });

        sb.endBlock('</map>');
    }

    /**
     *
     * @param {StringBuilder} sb
     * @param {Bean} bean
     * @returns {StringBuilder}
     */
    static _setProperties(sb, bean) {
        _.forEach(bean.properties, (prop, idx) => {
            switch (prop.clsName) {
                case 'DATA_SOURCE':
                    const valAttr = prop.name === 'dataSource' ? 'ref' : 'value';

                    sb.append(`<property name="${prop.name}" ${valAttr}="${prop.id}"/>`);

                    break;
                case 'EVENT_TYPES':
                    sb.startBlock(`<property name="${prop.name}">`);

                    if (prop.eventTypes.length === 1) {
                        const evtGrp = _.head(prop.eventTypes);

                        sb.append(`<util:constant static-field="${evtGrp.class}.${evtGrp.label}"/>`);
                    }
                    else {
                        sb.startBlock('<list>');

                        _.forEach(prop.eventTypes, (evtGrp, ix) => {
                            ix > 0 && sb.emptyLine();

                            sb.append(`<!-- EventType.${evtGrp.label} -->`);

                            _.forEach(evtGrp.events, (event) =>
                                sb.append(`<util:constant static-field="${evtGrp.class}.${event}"/>`));
                        });

                        sb.endBlock('</list>');
                    }

                    sb.endBlock('</property>');

                    break;
                case 'ARRAY':
                case 'COLLECTION':
                    this._setCollection(sb, prop);

                    break;
                case 'MAP':
                    sb.startBlock(`<property name="${prop.name}">`);

                    this._constructMap(sb, prop);

                    sb.endBlock('</property>');

                    break;
                case 'java.util.Properties':
                    sb.startBlock(`<property name="${prop.name}">`);
                    sb.startBlock('<props>');

                    _.forEach(prop.entries, (entry) => {
                        sb.append(`<prop key="${entry.name}">${entry.value}</prop>`);
                    });

                    sb.endBlock('</props>');
                    sb.endBlock('</property>');

                    break;
                case 'BEAN':
                    sb.startBlock(`<property name="${prop.name}">`);

                    this.appendBean(sb, prop.value);

                    sb.endBlock('</property>');

                    break;
                default:
                    sb.append(`<property name="${prop.name}" value="${this._toObject(prop.clsName, prop.value)}"/>`);
            }

            this._emptyLineIfNeeded(sb, bean.properties, idx);
        });

        return sb;
    }

    /**
     * Build final XML.
     *
     * @param {Bean} cfg Ignite configuration.
     * @param {Object} targetVer Version of Ignite for generated project.
     * @param {Boolean} clientNearCaches
     * @returns {StringBuilder}
     */
    static igniteConfiguration(cfg, targetVer, clientNearCaches) {
        const available = versionService.since.bind(versionService, targetVer.ignite);

        const sb = new StringBuilder();

        // 0. Add header.
        sb.append('<?xml version="1.0" encoding="UTF-8"?>');
        sb.emptyLine();

        this.mainComment(sb);
        sb.emptyLine();

        // 1. Start beans section.
        sb.startBlock([
            '<beans xmlns="http://www.springframework.org/schema/beans"',
            '       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
            '       xmlns:util="http://www.springframework.org/schema/util"',
            '       xsi:schemaLocation="http://www.springframework.org/schema/beans',
            '                           http://www.springframework.org/schema/beans/spring-beans.xsd',
            '                           http://www.springframework.org/schema/util',
            '                           http://www.springframework.org/schema/util/spring-util.xsd">']);

        // 2. Add external property file
        if (this.hasProperties(cfg)) {
            this.commentBlock(sb, 'Load external properties file.');

            sb.startBlock('<bean id="placeholderConfig" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">');
            sb.append('<property name="location" value="classpath:secret.properties"/>');
            sb.endBlock('</bean>');

            sb.emptyLine();
        }

        // 3. Add data sources.
        const dataSources = this.collectDataSources(cfg);

        if (dataSources.length) {
            this.commentBlock(sb, 'Data source beans will be initialized from external properties file.');

            _.forEach(dataSources, (ds) => {
                this.appendBean(sb, ds, true);

                sb.emptyLine();
            });
        }

        _.forEach(clientNearCaches, (cache) => {
            this.commentBlock(sb, `Configuration of near cache for cache "${cache.name}"`);

            this.appendBean(sb, this.generator.cacheNearClient(cache, available), true);

            sb.emptyLine();
        });

        // 3. Add main content.
        this.appendBean(sb, cfg);

        // 4. Close beans section.
        sb.endBlock('</beans>');

        return sb;
    }

    static cluster(cluster, targetVer, client) {
        const cfg = this.generator.igniteConfiguration(cluster, targetVer, client);

        const clientNearCaches = client ? _.filter(cluster.caches, (cache) =>
            cache.cacheMode === 'PARTITIONED' && _.get(cache, 'clientNearConfiguration.enabled')) : [];

        return this.igniteConfiguration(cfg, targetVer, clientNearCaches);
    }
}
