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

class Controller {
    static $inject = ['$transclude', '$document'];

    /**
     * @param {ng.ITranscludeFunction} $transclude
     * @param {JQLite} $document
     */
    constructor($transclude, $document) {
        this.$transclude = $transclude;
        this.$document = $document;
    }

    $postLink() {
        this.$transclude((clone) => {
            this.clone = clone;
            this.$document.find('body').append(clone);
        });
    }

    $onDestroy() {
        this.clone.remove();
        this.clone = this.$document = null;
    }
}

export default function directive() {
    return {
        restrict: 'E',
        transclude: true,
        controller: Controller,
        scope: {}
    };
}
