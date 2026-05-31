

import {default as ListEditableController, ItemScope} from '../../controller';
import {ListEditableTransclude} from '../list-editable-transclude/directive';

const CUSTOM_EVENT_TYPE = '$ngModel.change';

/** 
 * Emits $ngModel.change event on every ngModel.$viewValue change
 */
export function ngModel<T>(): ng.IDirective {
    return {
        link(scope, el, attr, {ngModel, list}: {ngModel: ng.INgModelController, list?: ListEditableController<T>}) {
            if (!list)
                return;

            ngModel.$viewChangeListeners.push(() => {
                el[0].dispatchEvent(new CustomEvent(CUSTOM_EVENT_TYPE, {bubbles: true, cancelable: true}));
            });
        },
        require: {
            ngModel: 'ngModel',
            list: '?^listEditable'
        }
    };
}
/** 
 * Triggers $ctrl.save when any ngModel emits $ngModel.change event
 */
export function listEditableTransclude<T>(): ng.IDirective {
    return {
        link(scope: ItemScope<T>, el, attr, {list, transclude}: {list?: ListEditableController<T>, transclude: ListEditableTransclude<T>}) {
            if (attr.listEditableTransclude !== 'itemEdit')
                return;

            if (!list)
                return;

            let listener = (e) => {
                e.stopPropagation();
                scope.$evalAsync(() => {
                    if (scope.form.$valid) list.save(scope.item, list.id(scope.item, transclude.$index));
                });
            };

            el[0].addEventListener(CUSTOM_EVENT_TYPE, listener);

            scope.$on('$destroy', () => {
                el[0].removeEventListener(CUSTOM_EVENT_TYPE, listener);
                listener = null;
            });
        },
        require: {
            list: '?^listEditable',
            transclude: 'listEditableTransclude'
        }
    };
}
