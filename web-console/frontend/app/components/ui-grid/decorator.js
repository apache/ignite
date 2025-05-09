

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
