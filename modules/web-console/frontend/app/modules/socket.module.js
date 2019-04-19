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

import angular from 'angular';
import io from 'socket.io-client'; // eslint-disable-line no-unused-vars

angular
.module('ignite-console.socket', [
])
.provider('igniteSocketFactory', function() {
    let _options = {};

    /**
     * @param {Object} options Socket io options.
     */
    this.set = (options) => {
        _options = options;
    };

    function factory(socketFactory) {
        return function() {
            const ioSocket = io.connect(_options);

            return socketFactory({ioSocket});
        };
    }

    factory.$inject = ['socketFactory'];

    this.$get = factory;

    return this;
});
