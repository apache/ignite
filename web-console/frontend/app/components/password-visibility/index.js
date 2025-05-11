

import angular from 'angular';
import './style.scss';
import {directive as visibilityRoot} from './root.directive';
import {component as toggleButton} from './toggle-button.component';

export default angular
    .module('ignite-console.passwordVisibility', [])
    .directive('passwordVisibilityRoot', visibilityRoot)
    .component('passwordVisibilityToggleButton', toggleButton);
