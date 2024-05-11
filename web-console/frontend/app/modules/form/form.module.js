

import angular from 'angular';

// Field.
import placeholder from './field/bs-select-placeholder.directive';

// Validators.
import ipaddress from './validator/ipaddress.directive';
import javaKeywords from './validator/java-keywords.directive';
import javaPackageSpecified from './validator/java-package-specified.directive';
import javaBuiltInClass from './validator/java-built-in-class.directive';
import javaIdentifier from './validator/java-identifier.directive';
import javaPackageName from './validator/java-package-name.directive';
import propertyValueSpecified from './validator/property-value-specified.directive';
import propertyUnique from './validator/property-unique.directive';
import unique from './validator/unique.directive';
import uuid from './validator/uuid.directive';

// Helpers.
import igniteFormFieldInputAutofocus from './field/input/autofocus.directive';
import IgniteFormGUID from './services/FormGUID.service.js';

angular
.module('ignite-console.Form', [

])
// Field.
.directive('bsSelect', placeholder)
// Validators.
.directive('ipaddress', ipaddress)
.directive('javaKeywords', javaKeywords)
.directive('javaPackageSpecified', javaPackageSpecified)
.directive('javaBuiltInClass', javaBuiltInClass)
.directive('javaIdentifier', javaIdentifier)
.directive('javaPackageName', javaPackageName)
.directive('ignitePropertyValueSpecified', propertyValueSpecified)
.directive('ignitePropertyUnique', propertyUnique)
.directive('igniteUnique', unique)
.directive('uuid', uuid)
// Helpers.
.directive('igniteFormFieldInputAutofocus', igniteFormFieldInputAutofocus)

// Generator of globally unique identifier.
.service('IgniteFormGUID', IgniteFormGUID);
