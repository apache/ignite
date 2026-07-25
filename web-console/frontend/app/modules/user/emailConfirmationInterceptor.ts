

import {UIRouter} from '@uirouter/angularjs';

registerInterceptor.$inject = ['$httpProvider'];

export function registerInterceptor(http: ng.IHttpProvider) {
    emailConfirmationInterceptor.$inject = ['$q', '$injector'];

    function emailConfirmationInterceptor($q: ng.IQService, $injector: ng.auto.IInjectorService): ng.IHttpInterceptor {
        return {
            responseError(res) {
                if (res.status === 403 && res.data && res.data.code === 10104)
                    $injector.get<UIRouter>('$uiRouter').stateService.go('signup-confirmation', {email: res.data.email});

                return $q.reject(res);
            }
        };
    }

    http.interceptors.push(emailConfirmationInterceptor as ng.IHttpInterceptorFactory);
}
