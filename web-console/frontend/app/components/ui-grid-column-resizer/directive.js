

export default function() {
    return {
        priority: -200,
        restrict: 'A',
        require: '?^uiGrid',
        link($scope, $element) {
            $element.on('dblclick', function($event) {
                $event.stopImmediatePropagation();
            });
        }
    };
}
