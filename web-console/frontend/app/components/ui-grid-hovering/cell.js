

export default function() {
    return {
        priority: -200,
        restrict: 'A',
        require: '?^uiGrid',
        link($scope, $element) {
            if (!$scope.grid.options.enableHovering)
                return;

            // Apply hover when mousing in.
            $element.on('mouseover', () => {
                // Empty all isHovered because scroll breaks it.
                $scope.row.grid.api.core.getVisibleRows().forEach((row) => {
                    row.isHovered = false;
                });

                // Now set proper hover
                $scope.row.isHovered = true;

                $scope.$apply();
            });

            // Remove hover when mousing out.
            $element.on('mouseout', () => {
                $scope.row.isHovered = false;

                $scope.$apply();
            });
        }
    };
}
