/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
