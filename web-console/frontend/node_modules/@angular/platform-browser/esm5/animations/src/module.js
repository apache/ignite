import * as tslib_1 from "tslib";
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BROWSER_ANIMATIONS_PROVIDERS, BROWSER_NOOP_ANIMATIONS_PROVIDERS } from './providers';
/**
 * Exports `BrowserModule` with additional [dependency-injection providers](guide/glossary#provider)
 * for use with animations. See [Animations](guide/animations).
 * @publicApi
 */
var BrowserAnimationsModule = /** @class */ (function () {
    function BrowserAnimationsModule() {
    }
    BrowserAnimationsModule = tslib_1.__decorate([
        NgModule({
            exports: [BrowserModule],
            providers: BROWSER_ANIMATIONS_PROVIDERS,
        })
    ], BrowserAnimationsModule);
    return BrowserAnimationsModule;
}());
export { BrowserAnimationsModule };
/**
 * A null player that must be imported to allow disabling of animations.
 * @publicApi
 */
var NoopAnimationsModule = /** @class */ (function () {
    function NoopAnimationsModule() {
    }
    NoopAnimationsModule = tslib_1.__decorate([
        NgModule({
            exports: [BrowserModule],
            providers: BROWSER_NOOP_ANIMATIONS_PROVIDERS,
        })
    ], NoopAnimationsModule);
    return NoopAnimationsModule;
}());
export { NoopAnimationsModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9kdWxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci9hbmltYXRpb25zL3NyYy9tb2R1bGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IjtBQUFBOzs7Ozs7R0FNRztBQUNILE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDdkMsT0FBTyxFQUFDLGFBQWEsRUFBQyxNQUFNLDJCQUEyQixDQUFDO0FBRXhELE9BQU8sRUFBQyw0QkFBNEIsRUFBRSxpQ0FBaUMsRUFBQyxNQUFNLGFBQWEsQ0FBQztBQUU1Rjs7OztHQUlHO0FBS0g7SUFBQTtJQUNBLENBQUM7SUFEWSx1QkFBdUI7UUFKbkMsUUFBUSxDQUFDO1lBQ1IsT0FBTyxFQUFFLENBQUMsYUFBYSxDQUFDO1lBQ3hCLFNBQVMsRUFBRSw0QkFBNEI7U0FDeEMsQ0FBQztPQUNXLHVCQUF1QixDQUNuQztJQUFELDhCQUFDO0NBQUEsQUFERCxJQUNDO1NBRFksdUJBQXVCO0FBR3BDOzs7R0FHRztBQUtIO0lBQUE7SUFDQSxDQUFDO0lBRFksb0JBQW9CO1FBSmhDLFFBQVEsQ0FBQztZQUNSLE9BQU8sRUFBRSxDQUFDLGFBQWEsQ0FBQztZQUN4QixTQUFTLEVBQUUsaUNBQWlDO1NBQzdDLENBQUM7T0FDVyxvQkFBb0IsQ0FDaEM7SUFBRCwyQkFBQztDQUFBLEFBREQsSUFDQztTQURZLG9CQUFvQiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cbmltcG9ydCB7TmdNb2R1bGV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtCcm93c2VyTW9kdWxlfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyJztcblxuaW1wb3J0IHtCUk9XU0VSX0FOSU1BVElPTlNfUFJPVklERVJTLCBCUk9XU0VSX05PT1BfQU5JTUFUSU9OU19QUk9WSURFUlN9IGZyb20gJy4vcHJvdmlkZXJzJztcblxuLyoqXG4gKiBFeHBvcnRzIGBCcm93c2VyTW9kdWxlYCB3aXRoIGFkZGl0aW9uYWwgW2RlcGVuZGVuY3ktaW5qZWN0aW9uIHByb3ZpZGVyc10oZ3VpZGUvZ2xvc3NhcnkjcHJvdmlkZXIpXG4gKiBmb3IgdXNlIHdpdGggYW5pbWF0aW9ucy4gU2VlIFtBbmltYXRpb25zXShndWlkZS9hbmltYXRpb25zKS5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuQE5nTW9kdWxlKHtcbiAgZXhwb3J0czogW0Jyb3dzZXJNb2R1bGVdLFxuICBwcm92aWRlcnM6IEJST1dTRVJfQU5JTUFUSU9OU19QUk9WSURFUlMsXG59KVxuZXhwb3J0IGNsYXNzIEJyb3dzZXJBbmltYXRpb25zTW9kdWxlIHtcbn1cblxuLyoqXG4gKiBBIG51bGwgcGxheWVyIHRoYXQgbXVzdCBiZSBpbXBvcnRlZCB0byBhbGxvdyBkaXNhYmxpbmcgb2YgYW5pbWF0aW9ucy5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuQE5nTW9kdWxlKHtcbiAgZXhwb3J0czogW0Jyb3dzZXJNb2R1bGVdLFxuICBwcm92aWRlcnM6IEJST1dTRVJfTk9PUF9BTklNQVRJT05TX1BST1ZJREVSUyxcbn0pXG5leHBvcnQgY2xhc3MgTm9vcEFuaW1hdGlvbnNNb2R1bGUge1xufVxuIl19