

import template from './template.pug';
import ModalPreviewProject from '../modal-preview-project/service';

export class ButtonPreviewProject {
    static $inject = ['ModalPreviewProject'];

    constructor(private ModalPreviewProject: ModalPreviewProject) {}

    cluster: any;

    preview() {
        return this.ModalPreviewProject.open(this.cluster);
    }
}
export const component = {
    name: 'buttonPreviewProject',
    controller: ButtonPreviewProject,
    template,
    bindings: {
        cluster: '<'
    }
};
