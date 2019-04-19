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

const INDETERMINATE_CLASS = 'progress-line__indeterminate';
const COMPLETE_CLASS = 'progress-line__complete';

/**
 * @typedef {-1} IndeterminateValue
 */

/**
 * @typedef {1} CompleteValue
 */

/**
 * @typedef {IndeterminateValue|CompleteValue} ProgressLineValue
 */

export default class ProgressLine {
    /** @type {ProgressLineValue} */
    value;

    static $inject = ['$element'];

    /**
     * @param {JQLite} $element
     */
    constructor($element) {
        this.$element = $element;
    }

    /**
     * @param {{value: ng.IChangesObject<ProgressLineValue>}} changes
     */
    $onChanges(changes) {
        if (changes.value.currentValue === -1) {
            this.$element[0].classList.remove(COMPLETE_CLASS);
            this.$element[0].classList.add(INDETERMINATE_CLASS);
            return;
        }
        if (typeof changes.value.currentValue === 'number') {
            if (changes.value.currentValue === 1) this.$element[0].classList.add(COMPLETE_CLASS);
            this.$element[0].classList.remove(INDETERMINATE_CLASS);
        }
    }
}
