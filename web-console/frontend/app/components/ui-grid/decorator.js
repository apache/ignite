/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
            else if (row.groupHeader && self.options.multiSelect) {
                const childrenToToggle = row.treeNode.children.filter((child) => child.row.isSelected === row.isSelected);
                childrenToToggle.forEach((child) => uiGridSelectionService.toggleRowSelection(self, child.row, evt, self.options.multiSelect, self.options.noUnselect));
            }
            else
                uiGridSelectionService.toggleRowSelection(self, row, evt, self.options.multiSelect, self.options.noUnselect);
        };

        if (igniteGridTable)
            $scope.selectButtonClick = mySelectButtonClick;
    };
    return $delegate;
}];
