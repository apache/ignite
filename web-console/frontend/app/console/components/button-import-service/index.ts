

import angular from 'angular';
import {component} from './component';

export default angular
    .module('console.button-import-service', [])
    .component(component.name, component);
