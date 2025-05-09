

import template from './template.pug';
import QueryActionsButton from './controller';

export const component: ng.IComponentOptions = {
    controller: QueryActionsButton,
    template,
    bindings: {
        actions: '<',
        item: '<'
    }
};
