

const template = `
    <svg
        class='copy-input-value-button'
        ignite-icon='copy'
        ignite-copy-to-clipboard='{{ $ctrl.value }}'
        bs-tooltip=''
        data-title='{{::$ctrl.title}}'
        ng-show='$ctrl.value'
    ></svg>
`;

class CopyInputValueButtonController {
    /** @type {ng.INgModelController} */
    ngModel;

    /**
     * Tooltip title
     * @type {string}
     */
    title;

    static $inject = ['$element', '$compile', '$scope'];

    /**
     * @param {JQLite} $element
     * @param {ng.ICompileService} $compile
     * @param {ng.IScope} $scope
     */
    constructor($element, $compile, $scope) {
        this.$element = $element;
        this.$compile = $compile;
        this.$scope = $scope;
    }

    $postLink() {
        this.buttonScope = this.$scope.$new(true);
        this.buttonScope.$ctrl = this;
        this.$compile(template)(this.buttonScope, (clone) => {
            this.$element[0].parentElement.appendChild(clone[0]);
            this.buttonElement = clone;
        });
    }

    $onDestroy() {
        this.buttonScope.$ctrl = null;
        this.buttonScope.$destroy();
        this.buttonElement.remove();
        this.buttonElement = this.$element = this.ngModel = null;
    }

    get value() {
        return this.ngModel
            ? this.ngModel.$modelValue
            : void 0;
    }
}

export function directive() {
    return {
        scope: false,
        bindToController: {
            title: '@copyInputValueButton'
        },
        controller: CopyInputValueButtonController,
        require: {
            ngModel: 'ngModel'
        }
    };
}
