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

import angular from 'angular';

angular
    .module('ignite-console.ace', [])
    .constant('igniteAceConfig', {})
    .directive('igniteAce', ['igniteAceConfig', (aceConfig) => {
        if (angular.isUndefined(window.ace))
            throw new Error('ignite-ace need ace to work... (o rly?)');

        /**
         * Sets editor options such as the wrapping mode or the syntax checker.
         *
         * The supported options are:
         *
         *   <ul>
         *     <li>showGutter</li>
         *     <li>useWrapMode</li>
         *     <li>onLoad</li>
         *     <li>theme</li>
         *     <li>mode</li>
         *   </ul>
         *
         * @param acee
         * @param session ACE editor session.
         * @param {object} opts Options to be set.
         */
        const setOptions = (acee, session, opts) => {
            // Sets the ace worker path, if running from concatenated or minified source.
            if (angular.isDefined(opts.workerPath)) {
                const config = window.ace.acequire('ace/config');

                config.set('workerPath', opts.workerPath);
            }

            // Ace requires loading.
            _.forEach(opts.require, (n) => window.ace.acequire(n));

            // Boolean options.
            if (angular.isDefined(opts.showGutter))
                acee.renderer.setShowGutter(opts.showGutter);

            if (angular.isDefined(opts.useWrapMode))
                session.setUseWrapMode(opts.useWrapMode);

            if (angular.isDefined(opts.showInvisibles))
                acee.renderer.setShowInvisibles(opts.showInvisibles);

            if (angular.isDefined(opts.showIndentGuides))
                acee.renderer.setDisplayIndentGuides(opts.showIndentGuides);

            if (angular.isDefined(opts.useSoftTabs))
                session.setUseSoftTabs(opts.useSoftTabs);

            if (angular.isDefined(opts.showPrintMargin))
                acee.setShowPrintMargin(opts.showPrintMargin);

            // Commands.
            if (angular.isDefined(opts.disableSearch) && opts.disableSearch) {
                acee.commands.addCommands([{
                    name: 'unfind',
                    bindKey: {
                        win: 'Ctrl-F',
                        mac: 'Command-F'
                    },
                    exec: _.constant(false),
                    readOnly: true
                }]);
            }

            // Base options.
            if (angular.isString(opts.theme))
                acee.setTheme('ace/theme/' + opts.theme);

            if (angular.isString(opts.mode))
                session.setMode('ace/mode/' + opts.mode);

            if (angular.isDefined(opts.firstLineNumber)) {
                if (angular.isNumber(opts.firstLineNumber))
                    session.setOption('firstLineNumber', opts.firstLineNumber);
                else if (angular.isFunction(opts.firstLineNumber))
                    session.setOption('firstLineNumber', opts.firstLineNumber());
            }

            // Advanced options.
            if (angular.isDefined(opts.advanced)) {
                for (const key in opts.advanced) {
                    if (opts.advanced.hasOwnProperty(key)) {
                        // Create a javascript object with the key and value.
                        const obj = {name: key, value: opts.advanced[key]};

                        // Try to assign the option to the ace editor.
                        acee.setOption(obj.name, obj.value);
                    }
                }
            }

            // Advanced options for the renderer.
            if (angular.isDefined(opts.rendererOptions)) {
                for (const key in opts.rendererOptions) {
                    if (opts.rendererOptions.hasOwnProperty(key)) {
                        // Create a javascript object with the key and value.
                        const obj = {name: key, value: opts.rendererOptions[key]};

                        // Try to assign the option to the ace editor.
                        acee.renderer.setOption(obj.name, obj.value);
                    }
                }
            }

            // onLoad callbacks.
            _.forEach(opts.callbacks, (cb) => {
                if (angular.isFunction(cb))
                    cb(acee);
            });
        };

        return {
            restrict: 'EA',
            require: ['?ngModel', '?^form'],
            link: (scope, elm, attrs, [ngModel, form]) => {
                /**
                 * Corresponds the igniteAceConfig ACE configuration.
                 *
                 * @type object
                 */
                const options = aceConfig.ace || {};

                /**
                 * IgniteAceConfig merged with user options via json in attribute or data binding.
                 *
                 * @type object
                 */
                let opts = angular.extend({}, options, scope.$eval(attrs.igniteAce));

                /**
                 * ACE editor.
                 *
                 * @type object
                 */
                const acee = window.ace.edit(elm[0]);

                /**
                 * ACE editor session.
                 *
                 * @type object
                 * @see [EditSession]{@link http://ace.c9.io/#nav=api&api=edit_session}
                 */
                const session = acee.getSession();

                /**
                 * Reference to a change listener created by the listener factory.
                 *
                 * @function
                 * @see listenerFactory.onChange
                 */
                let onChangeListener;

                /**
                 * Creates a change listener which propagates the change event and the editor session
                 * to the callback from the user option onChange.
                 * It might be exchanged during runtime, if this happens the old listener will be unbound.
                 *
                 * @param callback Callback function defined in the user options.
                 * @see onChangeListener
                 */
                const onChangeFactory = (callback) => {
                    return (e) => {
                        const newValue = session.getValue();

                        if (ngModel && newValue !== ngModel.$viewValue &&
                                // HACK make sure to only trigger the apply outside of the
                                // digest loop 'cause ACE is actually using this callback
                                // for any text transformation !
                            !scope.$$phase && !scope.$root.$$phase)
                            scope.$eval(() => ngModel.$setViewValue(newValue));

                        if (angular.isDefined(callback)) {
                            scope.$evalAsync(() => {
                                if (angular.isFunction(callback))
                                    callback([e, acee]);
                                else
                                    throw new Error('ignite-ace use a function as callback');
                            });
                        }
                    };
                };

                attrs.$observe('readonly', (value) => acee.setReadOnly(!!value || value === ''));

                // Value Blind.
                if (ngModel) {
                    // Remove "ngModel" controller from parent form for correct dirty checks.
                    form && form.$removeControl(ngModel);

                    ngModel.$formatters.push((value) => {
                        if (angular.isUndefined(value) || value === null)
                            return '';

                        if (angular.isObject(value) || angular.isArray(value))
                            throw new Error('ignite-ace cannot use an object or an array as a model');

                        return value;
                    });

                    ngModel.$render = () => session.setValue(ngModel.$viewValue);

                    acee.on('change', () => ngModel.$setViewValue(acee.getValue()));
                }

                // Listen for option updates.
                const updateOptions = (current, previous) => {
                    if (current === previous)
                        return;

                    opts = angular.extend({}, options, scope.$eval(attrs.igniteAce));

                    opts.callbacks = [opts.onLoad];

                    // Also call the global onLoad handler.
                    if (opts.onLoad !== options.onLoad)
                        opts.callbacks.unshift(options.onLoad);

                    // Unbind old change listener.
                    session.removeListener('change', onChangeListener);

                    // Bind new change listener.
                    onChangeListener = onChangeFactory(opts.onChange);

                    session.on('change', onChangeListener);

                    setOptions(acee, session, opts);
                };

                scope.$watch(attrs.igniteAce, updateOptions, /* deep watch */ true);

                // Set the options here, even if we try to watch later,
                // if this line is missing things go wrong (and the tests will also fail).
                updateOptions(options);

                elm.on('$destroy', () => {
                    acee.session.$stopWorker();
                    acee.destroy();
                });

                scope.$watch(() => [elm[0].offsetWidth, elm[0].offsetHeight],
                    () => {
                        acee.resize();
                        acee.renderer.updateFull();
                    }, true);
            }
        };
    }]);
