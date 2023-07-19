

class Controller {
    static $inject = ['$transclude', '$document'];

    /**
     * @param {ng.ITranscludeFunction} $transclude
     * @param {JQLite} $document
     */
    constructor($transclude, $document) {
        this.$transclude = $transclude;
        this.$document = $document;
    }

    $postLink() {
        this.$transclude((clone) => {
            this.clone = clone;
            this.$document.find('body').append(clone);
        });
    }

    $onDestroy() {
        this.clone.remove();
        this.clone = this.$document = null;
    }
}

export default function directive() {
    return {
        restrict: 'E',
        transclude: true,
        controller: Controller,
        scope: {}
    };
}
