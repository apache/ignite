

import {IInputErrorNotifier} from '../../types';

const scrollIntoView = (() => {
    if (HTMLElement.prototype.scrollIntoViewIfNeeded)
        return (el: HTMLElement) => {el.scrollIntoViewIfNeeded();};
    return (el: HTMLElement) => {
        try {
            el.scrollIntoView({block: 'center'});
        }
        catch (ignored) {
            el.scrollIntoView();
        }
    };
})();

/**
 * Brings user attention to invalid form fields.
 * Use IgniteFormUtils.triggerValidation to trigger the event.
 */
export function directive($timeout) {
    return {
        require: ['ngModel', '?^^bsCollapseTarget', '?^^igniteFormField', '?formFieldSize', '?^^panelCollapsible'],
        link(scope, el, attr, [ngModel, bsCollapseTarget, igniteFormField, formFieldSize, panelCollapsible]) {
            const formFieldController: IInputErrorNotifier = igniteFormField || formFieldSize;

            let onBlur;

            scope.$on('$destroy', () => {
                el[0].removeEventListener('blur', onBlur);
                onBlur = null;
            });

            const off = scope.$on('$showValidationError', (e, target) => {
                if (target !== ngModel)
                    return;

                ngModel.$setTouched();

                bsCollapseTarget && bsCollapseTarget.open();
                panelCollapsible && panelCollapsible.open();

                if (!onBlur && formFieldController) {
                    onBlur = () => formFieldController.hideError();

                    el[0].addEventListener('blur', onBlur, {passive: true});
                }

                $timeout(() => {
                    scrollIntoView(el[0]);

                    if (!attr.bsSelect)
                        $timeout(() => el[0].focus(), 100);

                    formFieldController && formFieldController.notifyAboutError();
                });
            });
        }
    };
}

directive.$inject = ['$timeout'];
