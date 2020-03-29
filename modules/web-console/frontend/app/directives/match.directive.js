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

// Directive to enable validation to match specified value.
export default function() {
    return {
        require: {
            ngModel: 'ngModel'
        },
        scope: false,
        bindToController: {
            igniteMatch: '<'
        },
        controller: class {
            /** @type {ng.INgModelController} */
            ngModel;
            /** @type {string} */
            igniteMatch;

            $postLink() {
                this.ngModel.$overrideModelOptions({allowInvalid: true});
                this.ngModel.$validators.mismatch = (value) => value === this.igniteMatch;
            }

            /**
             * @param {{igniteMatch: ng.IChangesObject<string>}} changes
             */
            $onChanges(changes) {
                if ('igniteMatch' in changes) this.ngModel.$validate();
            }
        }
    };
}
