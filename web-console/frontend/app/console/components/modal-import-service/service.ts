
import angular from 'angular';
import {Cluster} from 'app/configuration/types';
import {DemoService} from 'app/modules/demo/Demo.module';

export default class ModalImportService {
    static $inject = ['$modal'];

    modalInstance: mgcrea.ngStrap.modal.IModal;

    constructor(private $modal: mgcrea.ngStrap.modal.IModalService) {}

    open(cluster: Cluster) {
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
                <modal-import-service
                    on-hide='$hide()'
                    cluster='::$ctrl.cluster'
                    is-demo='::$ctrl.isDemo'
                ></modal-import-service>
            `,
            show: false
        });
        return this.modalInstance.$promise.then((modal) => {
            this.modalInstance.show();
        });
    }
}
