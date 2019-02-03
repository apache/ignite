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

export default ['$delegate', 'uiGridSelectionService', ($delegate, uiGridSelectionService) => {
    $delegate[0].require = ['^uiGrid', '?^igniteGridTable'];
    $delegate[0].compile = () => ($scope, $el, $attr, [uiGridCtrl, igniteGridTable]) => {
        const self = uiGridCtrl.grid;

        $delegate[0].link($scope, $el, $attr, uiGridCtrl);

        const mySelectButtonClick = (row, evt) => {
            evt.stopPropagation();

            if (evt.shiftKey)
                uiGridSelectionService.shiftSelect(self, row, evt, self.options.multiSelect);
            else
                uiGridSelectionService.toggleRowSelection(self, row, evt, self.options.multiSelect, self.options.noUnselect);
        };

        if (igniteGridTable)
            $scope.selectButtonClick = mySelectButtonClick;
    };
    return $delegate;
}];
