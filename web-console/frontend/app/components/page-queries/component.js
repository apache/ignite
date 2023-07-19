

import templateUrl from './template.tpl.pug';

export default {
    templateUrl,
    transclude: {
        queriesButtons: '?queriesButtons',
        queriesContent: '?queriesContent',
        queriesTitle: '?queriesTitle'
    },
    controller: class Ctrl {
        static $inject = ['$element', '$state', 'IgniteNotebook'];

        /**
         * @param {JQLite} $element       
         * @param {import('@uirouter/angularjs').StateService} $state         
         * @param {import('./notebook.service').default} IgniteNotebook
         */
        constructor($element, $state, IgniteNotebook) {
            this.$element = $element;
            this.$state = $state;
            this.IgniteNotebook = IgniteNotebook;
        }

        $onInit() {
            this.loadNotebooks();
        }

        async loadNotebooks() {
            const fetchNotebooksPromise = this.IgniteNotebook.read();

            this.notebooks = await fetchNotebooksPromise || [];
        }

        $postLink() {
            this.queriesTitle = this.$element.find('.queries-title');
            this.queriesButtons = this.$element.find('.queries-buttons');
            this.queriesContent = this.$element.find('.queries-content');
        }
    }
};
