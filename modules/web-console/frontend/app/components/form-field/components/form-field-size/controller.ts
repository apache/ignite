/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import get from 'lodash/get';
import {IInputErrorNotifier} from '../../../../types';

interface ISizeTypeOption {
    label: string,
    value: number
}

type ISizeType = Array<ISizeTypeOption>;

interface ISizeTypes {
    [name: string]: ISizeType
}

export default class PCFormFieldSizeController<T> implements IInputErrorNotifier {
    ngModel: ng.INgModelController;
    min?: number;
    max?: number;
    onScaleChange: ng.ICompiledExpression;
    innerForm: ng.IFormController;
    autofocus?: boolean;
    id = Math.random();
    inputElement?: HTMLInputElement;
    sizesMenu?: Array<ISizeTypeOption>;
    private _sizeScale: ISizeTypeOption;
    value: number;

    static $inject = ['$element', '$attrs'];

    static sizeTypes: ISizeTypes = {
        bytes: [
            {label: 'Kb', value: 1024},
            {label: 'Mb', value: 1024 * 1024},
            {label: 'Gb', value: 1024 * 1024 * 1024}
        ],
        gigabytes: [
            {label: 'Gb', value: 1},
            {label: 'Tb', value: 1024}
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

    constructor(private $element: JQLite, private $attrs: ng.IAttributes) {}

    $onDestroy() {
        delete this.$element[0].focus;
        this.$element = this.inputElement = null;
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
        this.inputElement = this.$element[0].querySelector('input');
        this.$element[0].focus = () => this.inputElement.focus();
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

    set sizeScale(value: ISizeTypeOption) {
        this._sizeScale = value;
        if (this.onScaleChange) this.onScaleChange({$event: this.sizeScale});
        if (this.ngModel) this.assignValue(this.ngModel.$viewValue);
    }

    get sizeScale() {
        return this._sizeScale;
    }

    assignValue(rawValue: number) {
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

    isTooltipValidation(): boolean {
        return !this.$element.parents('.theme--ignite-errors-horizontal').length;
    }

    notifyAboutError() {
        if (this.$element && this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseenter');
    }

    hideError() {
        if (this.$element && this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseleave');
    }

    triggerBlur() {
        this.$element[0].dispatchEvent(new FocusEvent('blur', {relatedTarget: this.inputElement}));
    }
}
