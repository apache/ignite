

import {nonEmpty} from 'app/utils/lodashMixins';

import {ListEditableColsController} from './cols.directive';

/** @returns {ng.IDirective} */
export default function() {
    return {
        require: '?^listEditableCols',
        /** @param {ListEditableColsController} ctrl */
        link(scope, el, attr, ctrl) {
            if (!ctrl || !ctrl.colDefs.length)
                return;

            const children = el.children();

            if (children.length !== ctrl.colDefs.length)
                return;

            if (ctrl.rowClass)
                el.addClass(ctrl.rowClass);

            ctrl.colDefs.forEach(({ cellClass }, index) => {
                if (nonEmpty(cellClass))
                    children[index].classList.add(...(Array.isArray(cellClass) ? cellClass : cellClass.split(' ')));
            });
        }
    };
}
