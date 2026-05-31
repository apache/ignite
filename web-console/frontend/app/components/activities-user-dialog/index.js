

import controller from './activities-user-dialog.controller';
import templateUrl from './activities-user-dialog.tpl.pug';

/**
 * @param {mgcrea.ngStrap.modal.IModalService} $modal
 */
export default function service($modal) {
    return function({ show = true, user }) {
        const ActivitiesUserDialog = $modal({
            templateUrl,
            show,
            resolve: {
                user: () => user
            },
            controller,
            controllerAs: 'ctrl'
        });

        return ActivitiesUserDialog.$promise
             .then(() => ActivitiesUserDialog);
    };
}

service.$inject = ['$modal'];
