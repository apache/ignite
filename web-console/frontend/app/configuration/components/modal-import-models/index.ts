

import angular from 'angular';
import {component} from './component';
import {component as importerFromTemplate} from './importer_from_template';
import service from './service';
import {component as stepIndicator} from './step-indicator/component';
import {component as tablesActionCell} from './tables-action-cell/component';
import {component as amountIndicator} from './selected-items-amount-indicator/component';

export default angular
    .module('configuration.modal-import-models', [])
    .service('ModalImportModels', service)
    .component('tablesActionCell', tablesActionCell)
    .component('modalImportModelsStepIndicator', stepIndicator)
    .component('selectedItemsAmountIndicator', amountIndicator)
    .component('modalImportModels', component)
    .component('modalImportModelsFromTemplate', importerFromTemplate);
