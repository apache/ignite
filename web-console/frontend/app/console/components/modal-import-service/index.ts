

import 'brace/mode/properties';
import 'brace/mode/yaml';
import angular from 'angular';
import component from './component';
import service from './service';

export default angular
    .module('ignite-console.console.modal-import-service', [])
    .service('ModalImportService', service)
    .component(component.name, component);
