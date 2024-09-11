

import _ from 'lodash';
import get from 'lodash/get';
import {IInputErrorNotifier} from '../../../../types';

interface ISizeTypeOption {
    label: string,
    value: number,
    translationId: string
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
    inputDebounce = 0;

    static $inject = ['$element', '$attrs'];

    static sizeTypes: ISizeTypes = {
        bytes: [
            {label: 'Kb', translationId: 'scale.digital.kilobyte.short', value: 1024},
            {label: 'Mb', translationId: 'scale.digital.megabyte.short', value: 1024 * 1024},
            {label: 'Gb', translationId: 'scale.digital.gigabyte.short', value: 1024 * 1024 * 1024}
        ],
        gigabytes: [
            {label: 'Gb', translationId: 'scale.digital.gigabyte.short', value: 1},
            {label: 'Tb', translationId: 'scale.digital.terabyte.short', value: 1024}
        ],
        seconds: [
            {label: 'ns', translationId: 'scale.time.nanosecond.short', value: 1 / 1000},
            {label: 'ms', translationId: 'scale.time.millisecond.short', value: 1},
            {label: 's', translationId: 'scale.time.second.short', value: 1000}
        ],
        time: [
            {label: 'sec', translationId: 'scale.time.second.medium', value: 1},
            {label: 'min', translationId: 'scale.time.minute.medium', value: 60},
            {label: 'hour', translationId: 'scale.time.hour.long', value: 60 * 60}
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
        this.ngModel.$render = () => {
            const rawValue = this.ngModel.$viewValue;

            if (rawValue) {
                this._sizeScale = _.findLast(this.sizesMenu,
                    (val) => val.value <= rawValue && Number.isInteger(rawValue / val.value));
            }

            this.assignValue(rawValue);
        };
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
        const oldScale = this._sizeScale;
        this._sizeScale = value;
        if (this.onScaleChange) this.onScaleChange({$event: this.sizeScale});

        if (this.ngModel)
            this.assignValue(oldScale ? this.ngModel.$viewValue / oldScale.value * value.value : this.ngModel.$viewValue);
    }

    get sizeScale() {
        return this._sizeScale;
    }

    assignValue(rawValue: number) {
        if (!this.sizesMenu) this.setDefaultSizeType();

        if (rawValue && rawValue !== this.ngModel.$viewValue)
            this.ngModel.$setViewValue(rawValue);

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
