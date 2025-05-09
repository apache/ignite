

import angular from 'angular';
import component from './component';

export default angular
    .module('ignite-console.version-picker', [
        'ignite-console.services'
    ])
    .component('versionPicker', component);
