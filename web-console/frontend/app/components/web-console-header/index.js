

import angular from 'angular';
import component from './component';
import userMenu from './components/user-menu/component';
import {component as content} from './components/web-console-header-content/component';
import {component as demo} from './components/demo-mode-button/component';
import {component as helpMenu} from './components/help-menu/component';

export default angular
    .module('ignite-console.web-console-header', [])
    .component('webConsoleHeader', component)
    .component('webConsoleHeaderContent', content)
    .component('helpMenu', helpMenu)
    .component('userMenu', userMenu)
    .component('demoModeButton', demo);
