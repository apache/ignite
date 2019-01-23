/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default class ModalPreviewProject {
    static $inject = ['$modal'];
    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     */
    constructor($modal) {
        this.$modal = $modal;
    }
    /**
     * @param {ig.config.cluster.ShortCluster} cluster
     */
    open(cluster) {
        this.modalInstance = this.$modal({
            locals: {
                cluster
            },
            controller: ['cluster', '$rootScope', function(cluster, $rootScope) {
                this.cluster = cluster;
                this.isDemo = !!$rootScope.IgniteDemoMode;
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
