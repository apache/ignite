

import angular from 'angular';

import component from './component';
import {reducer} from './reducer';

export default angular
    .module('ignite-console.page-console-basic', [])
    .run(['ConfigureState', (ConfigureState) => ConfigureState.addReducer((state, action) => Object.assign(state, {
        configureBasic: reducer(state.configureBasic, action, state)
    }))])
    .component('pageConsoleBasic', component);
