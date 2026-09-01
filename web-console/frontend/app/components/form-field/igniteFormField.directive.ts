
import {IInputErrorNotifier} from 'app/types';

type IgniteFormFieldScope < T > = ng.IScope & ({$input: T} | {[name: string]: T});

export class IgniteFormField<T> implements IInputErrorNotifier {
    static animName = 'ignite-form-field__error-blink';
    static eventName = 'webkitAnimationEnd oAnimationEnd msAnimationEnd animationend';
    static $inject = ['$element', '$scope'];
    onAnimEnd: () => any | null;

    constructor(private $element: JQLite, private $scope: IgniteFormFieldScope<T>) {}

    $postLink() {
        this.onAnimEnd = () => this.$element.removeClass(IgniteFormField.animName);
        this.$element.on(IgniteFormField.eventName, this.onAnimEnd);
    }

    $onDestroy() {
        this.$element.off(IgniteFormField.eventName, this.onAnimEnd);
        this.$element = this.onAnimEnd = null;
    }

    notifyAboutError() {
        if (!this.$element)
            return;

        if (this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseenter');
        else
            this.$element.addClass(IgniteFormField.animName);
    }

    hideError() {
        if (!this.$element)
            return;

        if (this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseleave');
    }

    isTooltipValidation(): boolean {
        return !this.$element.parents('.theme--ignite-errors-horizontal').length;
    }

    /**
     * Exposes control in $scope
     */
    exposeControl(control: ng.INgModelController, name = '$input') {
        this.$scope[name] = control;
        this.$scope.$on('$destroy', () => this.$scope[name] = null);
    }
}

export function directive<T>(): ng.IDirective<IgniteFormFieldScope<T>> {
    return {
        restrict: 'C',
        controller: IgniteFormField,
        scope: true
    };
}
