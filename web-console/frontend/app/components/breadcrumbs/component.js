

import template from './template.pug';
import './style.scss';

export class Breadcrumbs {
    static $inject = ['$transclude', '$element'];
    /**
     * @param {ng.ITranscludeFunction} $transclude
     * @param {JQLite} $element
     */
    constructor($transclude, $element) {
        this.$transclude = $transclude;
        this.$element = $element;
    }
    $postLink() {
        this.$transclude((clone) => {
            clone.first().prepend(this.$element.find('.breadcrumbs__home'));
            this.$element.append(clone);
        });
    }
}

export default {
    controller: Breadcrumbs,
    template,
    transclude: true
};
