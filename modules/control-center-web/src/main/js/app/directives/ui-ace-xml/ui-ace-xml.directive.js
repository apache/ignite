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

import template from './ui-ace-xml.jade!';
import controller from './ui-ace-xml.controller';

export default ['igniteUiAceXml', ['GeneratorXml', (xml) => {
    const link = (scope, $el, attrs, [ctrl, igniteUiAce]) => {
        if (typeof attrs.clusterCfg !== 'undefined') {
            scope.$watch('cfg', (cfg) => {
                if (typeof cfg !== 'undefined')
                    return;

                scope.cfg = {};
            });
        }

        if (igniteUiAce && igniteUiAce.onLoad)
            scope.onLoad = igniteUiAce.onLoad;

        if (igniteUiAce && igniteUiAce.onChange)
            scope.onChange = igniteUiAce.onChange;

        const generator = (data) => {
            delete ctrl.data;

            if (!data)
                return;

            return ctrl.generator(scope.cluster);
        };

        // Setup watchers.
        scope.$watch('generator', (method) => {
            if (method) {
                switch (method) {
                    case 'clusterCaches':
                        ctrl.generator = (cluster) => {
                            let caches;

                            caches = _.reduce(scope.caches, (acc, cache) => {
                                if (_.contains(cluster.caches, cache.value))
                                    acc.push(cache.cache);

                                return caches;
                            }, []);

                            return xml.clusterCaches(caches, null, true, xml.clusterGeneral(cluster)).asString();
                        };

                        break;

                    case 'igfss':
                        ctrl.generator = () => xml[method](scope.igfss).asString();

                        break;

                    default:
                        ctrl.generator = (cluster) => xml[method](cluster).asString();
                }
            }
        });
        scope.$watch('cfg', (data) => ctrl.data = generator(data), true);
        scope.$watch('cluster', (data) => ctrl.data = generator(data), true);
    };

    return {
        restrict: 'E',
        scope: {
            caches: '=',
            igfss: '=',

            generator: '@',
            cluster: '=',
            cfg: '=clusterCfg'
        },
        bindToController: {
            data: '=ngModel'
        },
        link,
        template,
        controller,
        controllerAs: 'ctrl',
        require: ['igniteUiAceXml', '?^igniteUiAce']
    };
}]];
