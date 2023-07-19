

import template from './template.pug';
import './style.scss';
import {AppStore, toggleSidebar} from '../../store';
import {UserService} from '../../modules/user/User.service';
import {map} from 'rxjs/operators';

export default {
    template,
    controller: class {
        static $inject = ['User', '$scope', '$state', 'IgniteBranding', 'UserNotifications', 'Store'];

        constructor(private User: UserService, $scope, $state, branding, UserNotifications, private store: AppStore) {
            Object.assign(this, {$scope, $state, branding, UserNotifications});
        }

        toggleSidebar() {
            this.store.dispatch(toggleSidebar());
        }

        userIsAuthorized$ = this.User.current$.pipe(map((user) => !!user))
    },
    transclude: true,
    bindings: {
        hideMenuButton: '<?'
    }
};
