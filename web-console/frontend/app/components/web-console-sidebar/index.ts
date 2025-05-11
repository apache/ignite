

import {component as overflow} from './web-console-sidebar-overflow/component';
import {component as nav} from './web-console-sidebar-navigation/component';
import {component as sidebar} from './component';

export default angular.module('sidebar', [])
    .component('webConsoleSidebarOverflow', overflow)
    .component('webConsoleSidebarNavigation', nav)
    .component('webConsoleSidebar', sidebar);
