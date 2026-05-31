

// eslint-disable-next-line
import {default as Panel} from './controller';

export default function panelCollapsibleTransclude() {
    return {
        restrict: 'A',
        require: {
            panel: '^panelCollapsible'
        },
        scope: true,
        controller: class {
            /** @type {Panel} */
            panel;
            /** @type {string} */
            slot;
            static $inject = ['$element'];
            /**
             * @param {JQLite} $element
             */
            constructor($element) {
                this.$element = $element;
            }
            $postLink() {
                this.panel.$transclude((clone, scope) => {
                    scope.$panel = this.panel;
                    this.$element.append(clone);
                }, null, this.slot);
            }
        },
        bindToController: {
            slot: '@panelCollapsibleTransclude'
        }
    };
}
