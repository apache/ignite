

import {UserService} from '../../modules/user/User.service';
import {DemoService} from 'app/modules/demo/Demo.module';
import {default as AdminData} from 'app/core/admin/Admin.data';
import AgentManager from 'app/modules/agent/AgentManager.service';

export default class PermanentNotifications {
    static $inject = ['Demo', 'UserNotifications', 'User', '$window', 'IgniteAdminData','AgentManager'];

    constructor(
        private Demo: DemoService,
        private UserNotifications: unknown,
        private User: UserService,
        private $window: ng.IWindowService,
        private AdminData: AdminData,
        private AgentManager: AgentManager
    ) {}

    user$ = this.User.current$;

    closeDemo() {
        this.$window.close();
        this.AgentManager.stopCluster({id:'demo-server',name:'demo-server',demo:true})
    }

    revertIdentity() {
        this.AdminData.revertIdentity();
    }
}
