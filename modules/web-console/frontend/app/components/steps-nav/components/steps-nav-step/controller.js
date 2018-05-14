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

export default class StepsNavStep {
    /** @type {import('../../controller').default} */
    stepsNav;
    /** @type {string} */
    value;
    /** @type {number} Step index among other steps */
    index;

    static $inject = ['$element'];

    /** 
     * @param {JQLite} $element
     */
    constructor($element) {
        this.$element = $element;
    }

    $onInit() {
        this.stepsNav.connectStep(this.value, Array.from(this.$element[0].parentElement.children).indexOf(this.$element[0]));
    }

    $onDestroy() {
        this.stepsNav.disconnectStep(this.value);
        this.$element = null;
    }

    get visited() {
        return this.stepsNav.isVisited(this.value);
    }

    get active() {
        return this.stepsNav.isActive(this.value);
    }
}
