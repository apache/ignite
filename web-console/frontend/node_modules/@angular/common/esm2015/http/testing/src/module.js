/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { HttpBackend, HttpClientModule } from '@angular/common/http';
import { NgModule } from '@angular/core';
import { HttpTestingController } from './api';
import { HttpClientTestingBackend } from './backend';
/**
 * Configures `HttpClientTestingBackend` as the `HttpBackend` used by `HttpClient`.
 *
 * Inject `HttpTestingController` to expect and flush requests in your tests.
 *
 * \@publicApi
 */
export class HttpClientTestingModule {
}
HttpClientTestingModule.decorators = [
    { type: NgModule, args: [{
                imports: [
                    HttpClientModule,
                ],
                providers: [
                    HttpClientTestingBackend,
                    { provide: HttpBackend, useExisting: HttpClientTestingBackend },
                    { provide: HttpTestingController, useExisting: HttpClientTestingBackend },
                ],
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9kdWxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL2h0dHAvdGVzdGluZy9zcmMvbW9kdWxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFdBQVcsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQ25FLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFdkMsT0FBTyxFQUFDLHFCQUFxQixFQUFDLE1BQU0sT0FBTyxDQUFDO0FBQzVDLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLFdBQVcsQ0FBQzs7Ozs7Ozs7QUFvQm5ELE1BQU0sT0FBTyx1QkFBdUI7OztZQVZuQyxRQUFRLFNBQUM7Z0JBQ1IsT0FBTyxFQUFFO29CQUNQLGdCQUFnQjtpQkFDakI7Z0JBQ0QsU0FBUyxFQUFFO29CQUNULHdCQUF3QjtvQkFDeEIsRUFBQyxPQUFPLEVBQUUsV0FBVyxFQUFFLFdBQVcsRUFBRSx3QkFBd0IsRUFBQztvQkFDN0QsRUFBQyxPQUFPLEVBQUUscUJBQXFCLEVBQUUsV0FBVyxFQUFFLHdCQUF3QixFQUFDO2lCQUN4RTthQUNGIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0h0dHBCYWNrZW5kLCBIdHRwQ2xpZW50TW9kdWxlfSBmcm9tICdAYW5ndWxhci9jb21tb24vaHR0cCc7XG5pbXBvcnQge05nTW9kdWxlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtIdHRwVGVzdGluZ0NvbnRyb2xsZXJ9IGZyb20gJy4vYXBpJztcbmltcG9ydCB7SHR0cENsaWVudFRlc3RpbmdCYWNrZW5kfSBmcm9tICcuL2JhY2tlbmQnO1xuXG5cbi8qKlxuICogQ29uZmlndXJlcyBgSHR0cENsaWVudFRlc3RpbmdCYWNrZW5kYCBhcyB0aGUgYEh0dHBCYWNrZW5kYCB1c2VkIGJ5IGBIdHRwQ2xpZW50YC5cbiAqXG4gKiBJbmplY3QgYEh0dHBUZXN0aW5nQ29udHJvbGxlcmAgdG8gZXhwZWN0IGFuZCBmbHVzaCByZXF1ZXN0cyBpbiB5b3VyIHRlc3RzLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW1xuICAgIEh0dHBDbGllbnRNb2R1bGUsXG4gIF0sXG4gIHByb3ZpZGVyczogW1xuICAgIEh0dHBDbGllbnRUZXN0aW5nQmFja2VuZCxcbiAgICB7cHJvdmlkZTogSHR0cEJhY2tlbmQsIHVzZUV4aXN0aW5nOiBIdHRwQ2xpZW50VGVzdGluZ0JhY2tlbmR9LFxuICAgIHtwcm92aWRlOiBIdHRwVGVzdGluZ0NvbnRyb2xsZXIsIHVzZUV4aXN0aW5nOiBIdHRwQ2xpZW50VGVzdGluZ0JhY2tlbmR9LFxuICBdLFxufSlcbmV4cG9ydCBjbGFzcyBIdHRwQ2xpZW50VGVzdGluZ01vZHVsZSB7XG59XG4iXX0=