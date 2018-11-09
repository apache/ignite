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

import get from 'lodash/get';

export default class PCFormFieldSizeController {
    /** @type {ng.INgModelController} */
    ngModel;
    /** @type {number} */
    min;
    /** @type {number} */
    max;
    /** @type {ng.ICompiledExpression} */
    onScaleChange;
    /** @type {ng.IFormController} */
    innerForm;

    static $inject = ['$element', '$attrs'];

    /** @type {ig.config.formFieldSize.ISizeTypes} */
    static sizeTypes = {
        bytes: [
            {label: 'Kb', value: 1024},
            {label: 'Mb', value: 1024 * 1024},
            {label: 'Gb', value: 1024 * 1024 * 1024}
        ],
        seconds: [
            {label: 'ns', value: 1 / 1000},
            {label: 'ms', value: 1},
            {label: 's', value: 1000}
        ],
        time: [
            {label: 'sec', value: 1},
            {label: 'min', value: 60},
            {label: 'hour', value: 60 * 60}
        ]
    };

    /**
     * @param {JQLite} $element
     * @param {ng.IAttributes} $attrs
     */
    constructor($element, $attrs) {
        this.$element = $element;
        this.$attrs = $attrs;
        this.id = Math.random();
    }

    $onDestroy() {
        this.$element = null;
    }

    $onInit() {
        if (!this.min) this.min = 0;
        if (!this.sizesMenu) this.setDefaultSizeType();
        this.$element.addClass('form-field');
        this.ngModel.$render = () => this.assignValue(this.ngModel.$viewValue);
    }

    $postLink() {
        if ('min' in this.$attrs)
            this.ngModel.$validators.min = (value) => this.ngModel.$isEmpty(value) || value === void 0 || value >= this.min;
        if ('max' in this.$attrs)
            this.ngModel.$validators.max = (value) => this.ngModel.$isEmpty(value) || value === void 0 || value <= this.max;

        this.ngModel.$validators.step = (value) => this.ngModel.$isEmpty(value) || value === void 0 || Math.floor(value) === value;
    }

    $onChanges(changes) {
        if ('sizeType' in changes) {
            this.sizesMenu = PCFormFieldSizeController.sizeTypes[changes.sizeType.currentValue];
            this.sizeScale = this.chooseSizeScale(get(changes, 'sizeScaleLabel.currentValue'));
        }
        if (!this.sizesMenu) this.setDefaultSizeType();
        if ('sizeScaleLabel' in changes)
            this.sizeScale = this.chooseSizeScale(changes.sizeScaleLabel.currentValue);

        if ('min' in changes) this.ngModel.$validate();
    }

    /**
     * @param {ig.config.formFieldSize.ISizeTypeOption} value
     */
    set sizeScale(value) {
        this._sizeScale = value;
        if (this.onScaleChange) this.onScaleChange({$event: this.sizeScale});
        if (this.ngModel) this.assignValue(this.ngModel.$viewValue);
    }

    get sizeScale() {
        return this._sizeScale;
    }

    /**
     * @param {number} rawValue
     */
    assignValue(rawValue) {
        if (!this.sizesMenu) this.setDefaultSizeType();
        return this.value = rawValue
            ? rawValue / this.sizeScale.value
            : rawValue;
    }

    onValueChange() {
        this.ngModel.$setViewValue(this.value ? this.value * this.sizeScale.value : this.value);
    }

    _defaultLabel() {
        if (!this.sizesMenu)
            return;

        return this.sizesMenu[1].label;
    }

    chooseSizeScale(label = this._defaultLabel()) {
        if (!label)
            return;

        return this.sizesMenu.find((option) => option.label.toLowerCase() === label.toLowerCase());
    }

    setDefaultSizeType() {
        this.sizesMenu = PCFormFieldSizeController.sizeTypes.bytes;
        this.sizeScale = this.chooseSizeScale();
    }
}
