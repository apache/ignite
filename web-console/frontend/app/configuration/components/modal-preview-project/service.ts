

import {ShortCluster} from '../../types';
import {DemoService} from 'app/modules/demo/Demo.module';

export default class ModalPreviewProject {
    static $inject = ['$modal'];

    modalInstance: mgcrea.ngStrap.modal.IModal;

    constructor(private $modal: mgcrea.ngStrap.modal.IModalService) {}

    open(cluster: ShortCluster) {
        this.modalInstance = this.$modal({
            locals: {
                cluster
            },
            controller: ['cluster', 'Demo', function(cluster, Demo: DemoService) {
                this.cluster = cluster;
                this.isDemo = !!Demo.enabled;
            }],
            controllerAs: '$ctrl',
            template: `
                <modal-preview-project
                    on-hide='$hide()'
                    cluster='::$ctrl.cluster'
                    is-demo='::$ctrl.isDemo'
                ></modal-preview-project>
            `,
            show: false
        });
        return this.modalInstance.$promise.then((modal) => {
            this.modalInstance.show();
        });
    }
}
