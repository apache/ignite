

import './style.scss';
import controller from './controller';
import templateUrl from './template.tpl.pug';
import { Stacktrace } from 'app/types/index';

export default class {
    static $inject = ['$modal'];

    constructor(private $modal: mgcrea.ngStrap.modal.IModalService) {}

    show(title: string, stacktrace: Stacktrace) {
        const modal = this.$modal({
            templateUrl,
            resolve: {
                title: () => title,
                stacktrace: () => stacktrace
            },
            controller,
            controllerAs: '$ctrl'
        });

        return modal.$promise;
    }
}
