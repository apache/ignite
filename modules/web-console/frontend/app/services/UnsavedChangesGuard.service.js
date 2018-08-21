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

const MSG = 'You have unsaved changes.\n\nAre you sure you want to discard them?';

// Service that show confirmation about unsaved changes on user change location.
export default ['IgniteUnsavedChangesGuard', ['$rootScope', function($root) {
    return {
        install(scope, customDirtyCheck = () => scope.ui.inputForm.$dirty) {
            scope.$on('$destroy', () => window.onbeforeunload = null);

            const unbind = $root.$on('$stateChangeStart', (event) => {
                if (_.get(scope, 'ui.inputForm', false) && customDirtyCheck()) {
                    if (!confirm(MSG)) // eslint-disable-line no-alert
                        event.preventDefault();
                    else
                        unbind();
                }
            });

            window.onbeforeunload = () => _.get(scope, 'ui.inputForm.$dirty', false) ? MSG : null;
        }
    };
}]];
