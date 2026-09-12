

import angular from 'angular';
import directive from './directive';
import flow from 'lodash/flow';

export default angular
    .module('ignite-console.ui-grid-filters', ['ui.grid'])
    .decorator('$tooltip', ['$delegate', ($delegate) => {
        return function(el, config) {
            const instance = $delegate(el, config);
            instance.$referenceElement = el;
            instance.destroy = flow(instance.destroy, () => instance.$referenceElement = null);
            instance.$applyPlacement = flow(instance.$applyPlacement, () => {
                if (!instance.$element)
                    return;

                const refWidth = instance.$referenceElement[0].getBoundingClientRect().width;
                const elWidth = instance.$element[0].getBoundingClientRect().width;
                if (refWidth > elWidth) {
                    instance.$element.css({
                        width: refWidth,
                        maxWidth: 'initial'
                    });
                }
            });
            return instance;
        };
    }])
    .directive('uiGridFilters', directive);
