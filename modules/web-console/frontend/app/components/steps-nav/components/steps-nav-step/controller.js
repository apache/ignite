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

// eslint-disable-next-line
import StepsNav from '../../controller';

export default class {
    /** @type {StepsNav} */
    stepsNav;
    /** @type {string} */
    value;
    /** @type {number} Step index among other steps */
    index;

    $onInit() {
        this.index = this.stepsNav.connectStep(this.value);
    }

    $onDestroy() {
        this.stepsNav.disconnectStep(this.value);
    }

    get visited() {
        return this.index <= this.stepsNav.steps.findIndex((value) => value === this.stepsNav.currentStep);
    }

    get first() {
        return this.index === 0;
    }

    get last() {
        return this.index + 1 === this.stepsNav.steps.length;
    }

    get active() {
        return this.stepsNav.currentStep === this.value;
    }
}
