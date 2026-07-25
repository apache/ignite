

export default class GlobalProgressLine {
    /** @type {boolean} */
    isLoading;

    static $inject = ['$element', '$document', '$scope'];

    _child: Element;

    constructor(private $element: JQLite, private $document: ng.IDocumentService, private $scope: ng.IScope) {}

    $onChanges() {
        this.$scope.$evalAsync(() => {
            if (this.isLoading) {
                this._child = this.$element[0].querySelector('.global-progress-line__progress-line');

                if (this._child)
                    this.$document[0].querySelector('web-console-header').appendChild(this._child);
            }
            else
                this.$element.hide();
        });
    }

    $onDestroy() {
        if (this._child) {
            this._child.parentElement.removeChild(this._child);
            this._child = null;
        }
    }
}
