

import template from './template.pug';
import controller from './controller';

import './style.scss';

export default {
    controller,
    template,
    require: {
        ngModel: '^ngModel'
    },
    bindings: {
    },
    transclude: {
        noItems: '?listEditableNoItems',
        itemView: '?listEditableItemView',
        itemEdit: '?listEditableItemEdit'
    }
};
