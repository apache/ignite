

import angular from 'angular';

import inputDialog from './input-dialog.service';

angular
    .module('ignite-console.input-dialog', [])
    .service('IgniteInput', inputDialog);
