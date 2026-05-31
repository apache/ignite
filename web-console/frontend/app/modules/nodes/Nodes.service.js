

import nodesDialogTplUrl from './nodes-dialog.tpl.pug';

const DEFAULT_OPTIONS = {
    grid: {
        multiSelect: false
    }
};

class Nodes {
    static $inject = ['$q', '$modal'];

    /**
     * @param {ng.IQService} $q
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     */
    constructor($q, $modal) {
        this.$q = $q;
        this.$modal = $modal;
    }

    selectNode(nodes, cacheName, options = DEFAULT_OPTIONS) {
        const { $q, $modal } = this;
        const defer = $q.defer();
        options.target = cacheName;

        const modalInstance = $modal({
            templateUrl: nodesDialogTplUrl,
            show: true,
            resolve: {
                nodes: () => nodes || [],
                options: () => options
            },
            controller: 'nodesDialogController',
            controllerAs: '$ctrl'
        });

        modalInstance.$scope.$ok = (data) => {
            defer.resolve(data);
            modalInstance.$scope.$hide();
        };

        modalInstance.$scope.$cancel = () => {
            defer.reject();
            modalInstance.$scope.$hide();
        };

        return defer.promise;
    }
}

export default Nodes;
