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
import AbstractTransformer from './AbstractTransformer';
import StringBuilder from './StringBuilder';

import ConfigurationGenerator from './ConfigurationGenerator';

import ClusterDefaults from './defaults/Cluster.service';
import CacheDefaults from './defaults/Cache.service';
import IGFSDefaults from './defaults/IGFS.service';

import JavaTypes from '../../../services/JavaTypes.service';

const generator = new ConfigurationGenerator();

const clusterDflts = new ClusterDefaults();
const cacheDflts = new CacheDefaults();
const igfsDflts = new IGFSDefaults();

const javaTypes = new JavaTypes(clusterDflts, cacheDflts, igfsDflts);

export default class SharpTransformer extends AbstractTransformer {
    static generator = generator;

    static commentBlock(sb, ...lines) {
        _.forEach(lines, (line) => sb.append(`// ${line}`));
    }

    static doc(sb, ...lines) {
        sb.append('/// <summary>');
        _.forEach(lines, (line) => sb.append(`/// ${line}`));
        sb.append('/// </summary>');
    }

    static mainComment(sb) {
        return this.doc(sb, sb.generatedBy());
    }

    /**
     *
     * @param {Array.<String>} sb
     * @param {Bean} bean
     */
    static _defineBean(sb, bean) {
        const shortClsName = javaTypes.shortClassName(bean.clsName);

        sb.append(`var ${bean.id} = new ${shortClsName}();`);
    }

    /**
     * @param {StringBuilder} sb
     * @param {Bean} parent
     * @param {Bean} propertyName
     * @param {String|Bean} value
     * @private
     */
    static _setProperty(sb, parent, propertyName, value) {
        sb.append(`${parent.id}.${_.upperFirst(propertyName)} = ${value};`);
    }

    /**
     *
     * @param {StringBuilder} sb
     * @param {Bean} parent
     * @param {String} propertyName
     * @param {Bean} bean
     * @private
     */
    static _setBeanProperty(sb, parent, propertyName, bean) {
        sb.append(`${parent.id}.${_.upperFirst(propertyName)} = ${bean.id};`);
    }

    static _toObject(clsName, val) {
        const items = _.isArray(val) ? val : [val];

        return _.map(items, (item, idx) => {
            if (_.isNil(item))
                return 'null';

            const shortClsName = javaTypes.shortClassName(clsName);

            switch (shortClsName) {
                // case 'byte':
                //     return `(byte) ${item}`;
                // case 'Serializable':
                case 'String':
                    if (items.length > 1)
                        return `"${item}"${idx !== items.length - 1 ? ' +' : ''}`;

                    return `"${item}"`;
                // case 'Path':
                //     return `"${item.replace(/\\/g, '\\\\')}"`;
                // case 'Class':
                //     return `${this.shortClassName(item)}.class`;
                // case 'UUID':
                //     return `UUID.fromString("${item}")`;
                // case 'PropertyChar':
                //     return `props.getProperty("${item}").toCharArray()`;
                // case 'Property':
                //     return `props.getProperty("${item}")`;
                // case 'Bean':
                //     if (item.isComplex())
                //         return item.id;
                //
                //     return this._newBean(item);
                default:
                    if (javaTypes.nonEnum(shortClsName))
                        return item;

                    return `${shortClsName}.${item}`;
            }
        });
    }

    /**
     *
     * @param {StringBuilder} sb
     * @param {Bean} bean
     * @returns {Array}
     */
    static _setProperties(sb = new StringBuilder(), bean) {
        _.forEach(bean.properties, (prop) => {
            switch (prop.clsName) {
                case 'ICollection':
                    // const implClsName = JavaTypes.shortClassName(prop.implClsName);

                    const colTypeClsName = javaTypes.shortClassName(prop.typeClsName);

                    if (colTypeClsName === 'String') {
                        const items = this._toObject(colTypeClsName, prop.items);

                        sb.append(`${bean.id}.${_.upperFirst(prop.name)} = new {${items.join(', ')}};`);
                    }
                    // else {
                    //     if (_.includes(vars, prop.id))
                    //         sb.append(`${prop.id} = new ${implClsName}<>();`);
                    //     else {
                    //         vars.push(prop.id);
                    //
                    //         sb.append(`${clsName}<${colTypeClsName}> ${prop.id} = new ${implClsName}<>();`);
                    //     }
                    //
                    //     sb.emptyLine();
                    //
                    //     if (nonBean) {
                    //         const items = this._toObject(colTypeClsName, prop.items);
                    //
                    //         _.forEach(items, (item) => {
                    //             sb.append(`${prop.id}.add("${item}");`);
                    //
                    //             sb.emptyLine();
                    //         });
                    //     }
                    //     else {
                    //         _.forEach(prop.items, (item) => {
                    //             this.constructBean(sb, item, vars, limitLines);
                    //
                    //             sb.append(`${prop.id}.add(${item.id});`);
                    //
                    //             sb.emptyLine();
                    //         });
                    //
                    //         this._setProperty(sb, bean.id, prop.name, prop.id);
                    //     }
                    // }

                    break;

                case 'Bean':
                    const nestedBean = prop.value;

                    this._defineBean(sb, nestedBean);

                    sb.emptyLine();

                    this._setProperties(sb, nestedBean);

                    sb.emptyLine();

                    this._setBeanProperty(sb, bean, prop.name, nestedBean);

                    break;
                default:
                    this._setProperty(sb, bean, prop.name, this._toObject(prop.clsName, prop.value));
            }
        });

        return sb;
    }

    /**
     * Build Java startup class with configuration.
     *
     * @param {Bean} cfg
     * @param pkg Package name.
     * @param clsName Class name for generate factory class otherwise generate code snippet.
     * @returns {String}
     */
    static toClassFile(cfg, pkg, clsName) {
        const sb = new StringBuilder();

        sb.startBlock(`namespace ${pkg}`, '{');

        _.forEach(_.sortBy(cfg.collectClasses()), (cls) => sb.append(`using ${cls};`));
        sb.emptyLine();


        this.mainComment(sb);
        sb.startBlock(`public class ${clsName}`, '{');

        this.doc(sb, 'Configure grid.');
        sb.startBlock('public static IgniteConfiguration CreateConfiguration()', '{');

        this._defineBean(sb, cfg);

        sb.emptyLine();

        this._setProperties(sb, cfg);

        sb.emptyLine();

        sb.append(`return ${cfg.id};`);

        sb.endBlock('}');

        sb.endBlock('}');

        sb.endBlock('}');

        return sb.asString();
    }

    static generateSection(bean) {
        const sb = new StringBuilder();

        this._setProperties(sb, bean);

        return sb.asString();
    }
}
