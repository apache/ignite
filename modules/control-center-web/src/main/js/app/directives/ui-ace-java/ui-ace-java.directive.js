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

import template from './ui-ace-java.jade!';
import controller from './ui-ace-java.controller';

export default ['igniteUiAceJava', ['GeneratorJava', (generator) => {
    const link = (scope, $el, attrs, [ctrl, igniteUiAceTabs, formCtrl, ngModelCtrl]) => {
        if (formCtrl && ngModelCtrl)
            formCtrl.$removeControl(ngModelCtrl);

        if (igniteUiAceTabs && igniteUiAceTabs.onLoad) {
            scope.onLoad = (editor) => {
                igniteUiAceTabs.onLoad(editor);

                scope.$watch('cluster', () => editor.attractAttention = false);
            };
        }

        if (igniteUiAceTabs && igniteUiAceTabs.onChange)
            scope.onChange = igniteUiAceTabs.onChange;

        const render = (data) => {
            delete ctrl.data;

            if (!data)
                return;

            return ctrl.generator(scope.cluster);
        };

        // Setup generator.
        if (scope.generator) {
            const method = scope.generator;

            switch (method) {
                case 'clusterCaches':
                    ctrl.generator = (cluster) => {
                        let caches;

                        caches = _.reduce(scope.caches, (acc, cache) => {
                            if (_.contains(cluster.caches, cache.value))
                                acc.push(cache.cache);

                            return acc;
                        }, []);

                        return generator.clusterCaches(caches, null, true, generator.clusterGeneral(cluster)).asString();
                    };

                    break;

                case 'clusterDiscovery':
                    ctrl.generator = (cluster) => generator.clusterDiscovery(cluster.discovery).asString();

                    break;

                case 'igfss':
                    ctrl.generator = (cluster) => {
                        let igfss;

                        igfss = _.reduce(scope.igfss, (acc, igfs) => {
                            if (_.contains(cluster.igfss, igfs.value))
                                acc.push(igfs.igfs);

                            return acc;
                        }, []);

                        return generator.igfss(igfss, 'cfg').asString();
                    };

                    break;

                default:
                    ctrl.generator = (cluster) => generator[method](cluster).asString();
            }
        }

        if (typeof attrs.clusterCfg !== 'undefined') {
            scope.$watch('cfg', (cfg) => {
                if (typeof cfg !== 'undefined')
                    return;

                scope.cfg = {};
            });

            scope.$watch('cfg', (data) => ctrl.data = render(data), true);
        }

        // Setup watchers.
        scope.$watch('cluster', (data) => ctrl.data = render(data), true);
    };

    return {
        priority: 1,
        restrict: 'E',
        scope: {
            caches: '=',
            igfss: '=',

            generator: '@',
            cluster: '=',
            cfg: '=?clusterCfg'
        },
        bindToController: {
            data: '=?ngModel'
        },
        link,
        template,
        controller,
        controllerAs: 'ctrl',
        require: ['igniteUiAceJava', '?^igniteUiAceTabs', '?^form', '?ngModel']
    };
}]];
