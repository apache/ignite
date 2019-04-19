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

import './style.scss';
import controller from './controller';
import templateUrl from './template.tpl.pug';

export default class {
    static $inject = ['$modal'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     */
    constructor($modal) {
        this.$modal = $modal;
    }

    show({ clusters }) {
        const modal = this.$modal({
            templateUrl,
            resolve: {
                clusters: () => clusters
            },
            controller,
            controllerAs: '$ctrl'
        });

        return modal.$promise;
    }
}
