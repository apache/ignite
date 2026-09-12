

const PASSWORD_VISIBLE_CLASS = 'password-visibility__password-visible';

export class PasswordVisibilityRoot {
    /** @type {ng.ICompiledExpression} */
    onPasswordVisibilityToggle;

    isVisible = false;
    static $inject = ['$element'];

    /**
     * @param {JQLite} $element
     */
    constructor($element) {
        this.$element = $element;
    }
    toggleVisibility() {
        this.isVisible = !this.isVisible;
        this.$element.toggleClass(PASSWORD_VISIBLE_CLASS, this.isVisible);
        if (this.onPasswordVisibilityToggle) this.onPasswordVisibilityToggle({$event: this.isVisible});
    }
}

export function directive() {
    return {
        restrict: 'A',
        scope: false,
        controller: PasswordVisibilityRoot,
        bindToController: {
            onPasswordVisibilityToggle: '&?'
        }
    };
}
