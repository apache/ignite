/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */
!function(e,t){"object"==typeof exports&&"undefined"!=typeof module?t(exports,require("@angular/common/http"),require("@angular/core"),require("rxjs")):"function"==typeof define&&define.amd?define("@angular/common/http/testing",["exports","@angular/common/http","@angular/core","rxjs"],t):t(((e=e||self).ng=e.ng||{},e.ng.common=e.ng.common||{},e.ng.common.http=e.ng.common.http||{},e.ng.common.http.testing={}),e.ng.common.http,e.ng.core,e.rxjs)}(this,function(e,t,r,n){"use strict";
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */var o=function o(){};function s(e,t,r,n){var o,s=arguments.length,i=s<3?t:null===n?n=Object.getOwnPropertyDescriptor(t,r):n;if("object"==typeof Reflect&&"function"==typeof Reflect.decorate)i=Reflect.decorate(e,t,r,n);else for(var u=e.length-1;u>=0;u--)(o=e[u])&&(i=(s<3?o(i):s>3?o(t,r,i):o(t,r))||i);return s>3&&i&&Object.defineProperty(t,r,i),i}
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
var i=function(){function e(e,t){this.request=e,this.observer=t,this._cancelled=!1}return Object.defineProperty(e.prototype,"cancelled",{get:function(){return this._cancelled},enumerable:!0,configurable:!0}),e.prototype.flush=function(e,r){if(void 0===r&&(r={}),this.cancelled)throw new Error("Cannot flush a cancelled request.");var n=this.request.urlWithParams,o=r.headers instanceof t.HttpHeaders?r.headers:new t.HttpHeaders(r.headers);e=function s(e,t){if(null===t)return null;switch(e){case"arraybuffer":return function r(e){if("undefined"==typeof ArrayBuffer)throw new Error("ArrayBuffer responses are not supported on this platform.");if(e instanceof ArrayBuffer)return e;throw new Error("Automatic conversion to ArrayBuffer is not supported for response type.")}(t);case"blob":return function n(e){if("undefined"==typeof Blob)throw new Error("Blob responses are not supported on this platform.");if(e instanceof Blob)return e;if(ArrayBuffer&&e instanceof ArrayBuffer)return new Blob([e]);throw new Error("Automatic conversion to Blob is not supported for response type.")}(t);case"json":return u(t);case"text":return function o(e){if("string"==typeof e)return e;if("undefined"!=typeof ArrayBuffer&&e instanceof ArrayBuffer)throw new Error("Automatic conversion to text is not supported for ArrayBuffers.");if("undefined"!=typeof Blob&&e instanceof Blob)throw new Error("Automatic conversion to text is not supported for Blobs.");return JSON.stringify(u(e,"text"))}(t);default:throw new Error("Unsupported responseType: "+e)}}
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */(this.request.responseType,e);var i=r.statusText,a=void 0!==r.status?r.status:200;if(void 0===r.status&&(null===e?(a=204,i=i||"No Content"):i=i||"OK"),void 0===i)throw new Error("statusText is required when setting a custom status.");a>=200&&a<300?(this.observer.next(new t.HttpResponse({body:e,headers:o,status:a,statusText:i,url:n})),this.observer.complete()):this.observer.error(new t.HttpErrorResponse({error:e,headers:o,status:a,statusText:i,url:n}))},e.prototype.error=function(e,r){if(void 0===r&&(r={}),this.cancelled)throw new Error("Cannot return an error for a cancelled request.");if(r.status&&r.status>=200&&r.status<300)throw new Error("error() called with a successful status.");var n=r.headers instanceof t.HttpHeaders?r.headers:new t.HttpHeaders(r.headers);this.observer.error(new t.HttpErrorResponse({error:e,headers:n,status:r.status||0,statusText:r.statusText||"",url:this.request.urlWithParams}))},e.prototype.event=function(e){if(this.cancelled)throw new Error("Cannot send events to a cancelled request.");this.observer.next(e)},e}();function u(e,t){if(void 0===t&&(t="JSON"),"undefined"!=typeof ArrayBuffer&&e instanceof ArrayBuffer)throw new Error("Automatic conversion to "+t+" is not supported for ArrayBuffers.");if("undefined"!=typeof Blob&&e instanceof Blob)throw new Error("Automatic conversion to "+t+" is not supported for Blobs.");if("string"==typeof e||"number"==typeof e||"object"==typeof e||Array.isArray(e))return e;throw new Error("Automatic conversion to "+t+" is not supported for response type.")}var a=function(){function e(){this.open=[]}return e.prototype.handle=function(e){var r=this;return new n.Observable(function(n){var o=new i(e,n);return r.open.push(o),n.next({type:t.HttpEventType.Sent}),function(){o._cancelled=!0}})},e.prototype._match=function(e){return this.open.filter("string"==typeof e?function(t){return t.request.urlWithParams===e}:"function"==typeof e?function(t){return e(t.request)}:function(t){return!(e.method&&t.request.method!==e.method.toUpperCase()||e.url&&t.request.urlWithParams!==e.url)})},e.prototype.match=function(e){var t=this,r=this._match(e);return r.forEach(function(e){var r=t.open.indexOf(e);-1!==r&&t.open.splice(r,1)}),r},e.prototype.expectOne=function(e,t){t=t||this.descriptionFromMatcher(e);var r=this.match(e);if(r.length>1)throw new Error('Expected one matching request for criteria "'+t+'", found '+r.length+" requests.");if(0===r.length)throw new Error('Expected one matching request for criteria "'+t+'", found none.');return r[0]},e.prototype.expectNone=function(e,t){t=t||this.descriptionFromMatcher(e);var r=this.match(e);if(r.length>0)throw new Error('Expected zero matching requests for criteria "'+t+'", found '+r.length+".")},e.prototype.verify=function(e){void 0===e&&(e={});var t=this.open;if(e.ignoreCancelled&&(t=t.filter(function(e){return!e.cancelled})),t.length>0){var r=t.map(function(e){var t=e.request.urlWithParams.split("?")[0];return e.request.method+" "+t}).join(", ");throw new Error("Expected no open requests, found "+t.length+": "+r)}},e.prototype.descriptionFromMatcher=function(e){return"string"==typeof e?"Match URL: "+e:"object"==typeof e?"Match method: "+(e.method||"(any)")+", URL: "+(e.url||"(any)"):"Match by function: "+e.name},s([r.Injectable()],e)}(),f=function(){return s([r.NgModule({imports:[t.HttpClientModule],providers:[a,{provide:t.HttpBackend,useExisting:a},{provide:o,useExisting:a}]})],function e(){})}();
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
e.ɵangular_packages_common_http_testing_testing_a=a,e.HttpTestingController=o,e.HttpClientTestingModule=f,e.TestRequest=i,Object.defineProperty(e,"__esModule",{value:!0})});