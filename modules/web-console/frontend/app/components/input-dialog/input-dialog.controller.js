/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import _ from 'lodash';

export default class InputDialogController {
    static $inject = ['deferred', 'ui'];

    constructor(deferred, options) {
        this.deferred = deferred;
        this.options = options;
    }

    confirm() {
        if (_.isFunction(this.options.toValidValue))
            return this.deferred.resolve(this.options.toValidValue(this.options.value));

        this.deferred.resolve(this.options.value);
    }
}
