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

import template from './ui-ace-sharp.pug';
import controller from './ui-ace-sharp.controller';

/**
 * @param {import('app/modules/configuration/generator/SharpTransformer.service').default} generator
 */
export default function directive(generator) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} $el
     * @param {ng.IAttributes} attrs
     * @param {[typeof controller, any?, ng.IFormController?, ng.INgModelController?]} controllers
     */
    const link = (scope, $el, attrs, controllers) => {
        const [ctrl, igniteUiAceTabs, formCtrl, ngModelCtrl] = controllers;
        if (formCtrl && ngModelCtrl)
            formCtrl.$removeControl(ngModelCtrl);

        if (igniteUiAceTabs && igniteUiAceTabs.onLoad) {
            scope.onLoad = (editor) => {
                igniteUiAceTabs.onLoad(editor);

                scope.$watch('master', () => editor.attractAttention = false);
            };
        }

        if (igniteUiAceTabs && igniteUiAceTabs.onChange)
            scope.onChange = igniteUiAceTabs.onChange;

        const render = (data) => {
            delete ctrl.data;

            if (!data)
                return;

            return ctrl.generator(scope.master);
        };

        // Setup generator.
        if (scope.generator) {
            const method = scope.generator;

            switch (method) {
                case 'clusterCaches':
                    ctrl.generator = (cluster) => {
                        const caches = _.reduce(scope.detail, (acc, cache) => {
                            if (_.includes(cluster.caches, cache.value))
                                acc.push(cache.cache);

                            return acc;
                        }, []);

                        return generator.clusterCaches(cluster, caches, null, true).asString();
                    };

                    break;

                case 'igfss':
                    ctrl.generator = (cluster) => {
                        const igfss = _.reduce(scope.detail, (acc, igfs) => {
                            if (_.includes(cluster.igfss, igfs.value))
                                acc.push(igfs.igfs);

                            return acc;
                        }, []);

                        return generator.igfss(igfss, 'cfg').asString();
                    };

                    break;

                case 'cacheStore':
                    ctrl.generator = (cache) => {
                        const domains = _.reduce(scope.detail, (acc, domain) => {
                            if (_.includes(cache.domains, domain.value))
                                acc.push(domain.meta);

                            return acc;
                        }, []);

                        return generator.cacheStore(cache, domains).asString();
                    };

                    break;

                default:
                    ctrl.generator = (data) => generator[method](data).asString();
            }
        }

        if (!_.isUndefined(attrs.clusterCfg)) {
            scope.$watch('cfg', (cfg) => {
                if (!_.isUndefined(cfg))
                    return;

                scope.cfg = {};
            });

            scope.$watch('cfg', (data) => ctrl.data = render(data), true);
        }

        const noDeepWatch = !(typeof attrs.noDeepWatch !== 'undefined');

        // Setup watchers.
        scope.$watch('master', (data) => ctrl.data = render(data), noDeepWatch);
    };

    return {
        priority: 1,
        restrict: 'E',
        scope: {
            master: '=',
            detail: '=',
            generator: '@',
            cfg: '=?clusterCfg'
        },
        bindToController: {
            data: '=?ngModel'
        },
        link,
        template,
        controller,
        controllerAs: 'ctrl',
        require: ['igniteUiAceSharp', '?^igniteUiAceTabs', '?^form', '?ngModel']
    };
}

directive.$inject = ['IgniteSharpTransformer'];
