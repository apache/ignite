import template from './template.pug';
import './style.scss';
import ModalImportModels from '../modal-import-models/service';

export class ButtonImportModels {
    static $inject = ['ModalImportModels'];

    constructor(private ModalImportModels: ModalImportModels) {}

    clusterId: string;

    startImport() {
        return this.ModalImportModels.open();
    }

    startImportFromTemplate() {
        return this.ModalImportModels.openTemplate();
    }
}
export const component = {
    name: 'buttonImportModels',
    controller: ButtonImportModels,
    template,
    bindings: {
        clusterID: '<clusterId'
    }
};
