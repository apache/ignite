

export default class {
    static $inject = ['$scope'];

    /**
     * @param {ng.IScope} $scope
     */
    constructor($scope) {
        this.$scope = $scope;
    }

    areAllSelected() {
        return this.$scope.$matches.every(({index}) => this.$scope.$isActive(index));
    }
}
