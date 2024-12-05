

import {PasswordVisibilityRoot} from './root.directive';

class Controller {
    /** @type {PasswordVisibilityRoot} */
    visibilityRoot;

    toggleVisibility() {
        this.visibilityRoot.toggleVisibility();
    }
    get isVisible() {
        return this.visibilityRoot.isVisible;
    }
}

export const component = {
    template: `
        <button
            type='button'
            ng-click='$ctrl.toggleVisibility()'
            bs-tooltip='{title: (($ctrl.isVisible ? "formField.passwordVisibility.hide" : "formField.passwordVisibility.show")|translate)}'
            data-placement='top'
        >
            <svg ignite-icon='eyeOpened' class='password-visibility__icon-visible'></svg>
            <svg ignite-icon='eyeClosed' class='password-visibility__icon-hidden'></svg>
        </button>
    `,
    require: {
        visibilityRoot: '^passwordVisibilityRoot'
    },
    controller: Controller
};
