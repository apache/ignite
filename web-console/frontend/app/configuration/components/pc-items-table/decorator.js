

export default ['$delegate', 'uiGridSelectionService', ($delegate, uiGridSelectionService) => {
    $delegate[0].require = ['^uiGrid', '?^pcItemsTable'];
    $delegate[0].compile = () => ($scope, $el, $attr, [uiGridCtrl, pcItemsTable]) => {
        const self = uiGridCtrl.grid;
        $delegate[0].link($scope, $el, $attr, uiGridCtrl);
        const mySelectButtonClick = (row, evt) => {
            evt.stopPropagation();

            if (evt.shiftKey)
                uiGridSelectionService.shiftSelect(self, row, evt, self.options.multiSelect);
            else
                uiGridSelectionService.toggleRowSelection(self, row, evt, self.options.multiSelect, self.options.noUnselect);
        };
        if (pcItemsTable) $scope.selectButtonClick = mySelectButtonClick;
    };
    return $delegate;
}];
