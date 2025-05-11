

import './style.scss';
import controller from './controller';
import templateUrl from './template.tpl.pug';

export default class {
    static $inject = ['$modal'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     */
    constructor($modal) {
        this.$modal = $modal;
    }

    show({ clusters }) {
        const modal = this.$modal({
            templateUrl,
            resolve: {
                clusters: () => clusters
            },
            controller,
            controllerAs: '$ctrl'
        });

        return modal.$promise;
    }
}
