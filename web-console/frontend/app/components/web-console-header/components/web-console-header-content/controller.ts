

import {StateService} from '@uirouter/angularjs';
import {UserService, User} from 'app/modules/user/User.service';
import {DemoService} from 'app/modules/demo/Demo.module';
import {tap} from 'rxjs/operators';

export default class WebConsoleHeaderContent {
    static $inject = ['Demo', '$state', 'User'];

    constructor(
        private Demo: DemoService,
        private $state: StateService,
        private User: UserService
    ) {}

    static connectedClusterInvisibleStates = [
        '403', '404', 'signin'
    ];

    user: User;

    _subscriber = this.User.current$.pipe(tap((user) => this.user = user)).subscribe();

    get showConnectedClusters(): boolean {
        return this.user &&
            !this.Demo.enabled &&
            !this.constructor.connectedClusterInvisibleStates.some((state) => this.$state.includes(state)) &&
            !this.user.becomeUsed;
    }

    get showUserMenu(): boolean {
        return !!this.user;
    }

    get showDemoModeButton(): boolean {
        return this.user && !this.user.becomeUsed && !this.Demo.enabled;
    }

    $onDestroy() {
        if (this._subscriber) this._subscriber.unsubscribe();
    }
}
