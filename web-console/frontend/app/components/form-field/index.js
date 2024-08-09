

import angular from 'angular';
import './style.scss';
import {directive as igniteFormField} from './igniteFormField.directive';
import {directive as showValidationError} from './showValidationError.directive';
import {directive as copyInputValue} from './copyInputValueButton.directive';

import { default as formFieldSize } from './components/form-field-size';

export default angular
    .module('ignite-console.form-field', [])
    .component('formFieldSize', formFieldSize)
    .directive('igniteFormField', igniteFormField)
    .directive('ngModel', showValidationError)
    .directive('copyInputValueButton', copyInputValue);
