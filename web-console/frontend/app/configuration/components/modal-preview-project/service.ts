/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
