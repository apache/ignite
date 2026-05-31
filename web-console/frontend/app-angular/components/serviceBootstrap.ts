

/**
 * This component ensures that Angular starts before AngularJS
 * https://github.com/ui-router/angular-hybrid/issues/98
 */

import {Component} from '@angular/core';
import angular from 'angular';

@Component({
    selector: 'service-bootstrap',
    template: ''
})
export class ServiceBootstrapComponent {
    constructor() {
        const injector = angular.element(document.body).injector();
        const urlService = injector.get('$urlService');
        urlService.listen();
        urlService.sync();
    }
}
