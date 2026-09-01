

import isMatch from 'lodash/isMatch';
import {default as ListEditableController, ID} from '../../controller';

export default function listEditableOneWay(): ng.IDirective {
    return {
        require: {
            list: 'listEditable'
        },
        bindToController: {
            onItemChange: '&?',
            onItemRemove: '&?'
        },
        controller: class Controller<T> {
            list: ListEditableController<T>;
            onItemChange: ng.ICompiledExpression;
            onItemRemove: ng.ICompiledExpression;

            $onInit() {
                this.list.save = (item: T, id: ID) => {
                    if (!isMatch(this.list.getItem(id), item)) this.onItemChange({$event: item});
                };
                this.list.remove = (id: ID) => this.onItemRemove({
                    $event: this.list.getItem(id)
                });
            }
        }
    };
}
