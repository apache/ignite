

export default class PanelCollapsible {
    /** @type {Boolean} */
    opened;
    /** @type {ng.ICompiledExpression} */
    onOpen;
    /** @type {ng.ICompiledExpression} */
    onClose;
    /** @type {String} */
    disabled;

    static $inject = ['$transclude'];

    /**
     * @param {ng.ITranscludeFunction} $transclude
     */
    constructor($transclude) {
        this.$transclude = $transclude;
    }

    toggle() {
        if (this.opened)
            this.close();
        else
            this.open();
    }

    open() {
        if (this.disabled)
            return;

        this.opened = true;

        if (this.onOpen && this.opened)
            this.onOpen({});
    }

    close() {
        if (this.disabled)
            return;

        this.opened = false;

        if (this.onClose && !this.opened)
            this.onClose({});
    }
}
