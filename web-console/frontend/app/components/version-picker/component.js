

import _ from 'lodash';
import template from './template.pug';
import './style.scss';

export default {
    template,
    controller: class {
        static $inject = ['IgniteVersion', '$scope'];

        /**
         * @param {import('app/services/Version.service').default} Version
         * @param {ng.IScope} $scope
         */
        constructor(Version, $scope) {
            this.currentSbj = Version.currentSbj;
            this.supportedVersions = Version.supportedVersions;

            const dropdownToggle = (active) => {
                this.isActive = active;

                // bs-dropdown does not call apply on callbacks
                $scope.$apply();
            };

            this.onDropdownShow = () => dropdownToggle(true);
            this.onDropdownHide = () => dropdownToggle(false);
        }

        $onInit() {
            this.dropdown = _.map(this.supportedVersions, (ver) => ({
                text: ver.label,
                click: () => this.currentSbj.next(ver)
            }));

            this.currentSbj.subscribe({
                next: (ver) => this.currentVersion = ver.label
            });
        }
    }
};
