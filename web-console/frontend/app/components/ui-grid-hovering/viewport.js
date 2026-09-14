

import angular from 'angular';

export default function() {
    return {
        priority: -200,
        compile($el) {
            let newNgClass = '';

            const rowRepeatDiv = angular.element($el.children().children()[0]);
            const existingNgClass = rowRepeatDiv.attr('ng-class');

            if (existingNgClass)
                newNgClass = existingNgClass.slice(0, -1) + ', "ui-grid-row-hovered": row.isHovered }';
            else
                newNgClass = '{ "ui-grid-row-hovered": row.isHovered }';

            rowRepeatDiv.attr('ng-class', newNgClass);

            return {
                pre() { },
                post() { }
            };
        }
    };
}
