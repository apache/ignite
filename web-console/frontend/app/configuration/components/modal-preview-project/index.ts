

import 'brace/mode/properties';
import 'brace/mode/yaml';
import angular from 'angular';
import component from './component';
import service from './service';

export default angular
    .module('ignite-console.page-configure.modal-preview-project', [])
    .service('ModalPreviewProject', service)
    .component(component.name, component);
