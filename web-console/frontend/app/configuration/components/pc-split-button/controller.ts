

type ActionMenuItem = {icon: string, text: string, click: ng.ICompiledExpression};
type ActionsMenu = ActionMenuItem[];

/**
 * Groups multiple buttons into a single button with all but first buttons in a dropdown
 */
export default class SplitButton {
    actions: ActionsMenu = [];

    static $inject = ['$element'];

    constructor(private $element: JQLite) {}

    $onInit() {
        this.$element[0].classList.add('btn-ignite-group');
    }
}
