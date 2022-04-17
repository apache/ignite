/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */
!function(t,e){"object"==typeof exports&&"undefined"!=typeof module?e(exports,require("@angular/compiler")):"function"==typeof define&&define.amd?define("@angular/compiler/testing",["exports","@angular/compiler"],e):e(((t=t||self).ng=t.ng||{},t.ng.compiler=t.ng.compiler||{},t.ng.compiler.testing={}),t.ng.compiler)}(this,function(t,e){"use strict";var o=function(t,e){return(o=Object.setPrototypeOf||{__proto__:[]}instanceof Array&&function(t,e){t.__proto__=e}||function(t,e){for(var o in e)e.hasOwnProperty(o)&&(t[o]=e[o])})(t,e)};function r(t,e){function r(){this.constructor=t}o(t,e),t.prototype=null===e?Object.create(e):(r.prototype=e.prototype,new r)}
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
var n=function(t){function e(){var e=null!==t&&t.apply(this,arguments)||this;return e._expectations=[],e._definitions=new Map,e._requests=[],e}return r(e,t),e.prototype.get=function(t){var e=new i(t);return this._requests.push(e),e.getPromise()},e.prototype.hasPendingRequests=function(){return!!this._requests.length},e.prototype.expect=function(t,e){var o=new s(t,e);this._expectations.push(o)},e.prototype.when=function(t,e){this._definitions.set(t,e)},e.prototype.flush=function(){if(0===this._requests.length)throw new Error("No pending requests to flush");do{this._processRequest(this._requests.shift())}while(this._requests.length>0);this.verifyNoOutstandingExpectations()},e.prototype.verifyNoOutstandingExpectations=function(){if(0!==this._expectations.length){for(var t=[],e=0;e<this._expectations.length;e++)t.push(this._expectations[e].url);throw new Error("Unsatisfied requests: "+t.join(", "))}},e.prototype._processRequest=function(t){var e=t.url;if(this._expectations.length>0){var o=this._expectations[0];if(o.url==e)return function r(t,e){var o=t.indexOf(e);o>-1&&t.splice(o,1)}
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */(this._expectations,o),void t.complete(o.response)}if(!this._definitions.has(e))throw new Error("Unexpected request "+e);var n=this._definitions.get(e);t.complete(null==n?null:n)},e}(e.ResourceLoader),i=function(){function t(t){var e=this;this.url=t,this.promise=new Promise(function(t,o){e.resolve=t,e.reject=o})}return t.prototype.complete=function(t){null==t?this.reject("Failed to load "+this.url):this.resolve(t)},t.prototype.getPromise=function(){return this.promise},t}(),s=function s(t,e){this.url=t,this.response=e},p=function(){function t(t,e,o,r,n){this.existingProperties=t,this.attrPropMapping=e,this.existingElements=o,this.invalidProperties=r,this.invalidAttributes=n}return t.prototype.hasProperty=function(t,e,o){var r=this.existingProperties[e];return void 0===r||r},t.prototype.hasElement=function(t,e){var o=this.existingElements[t.toLowerCase()];return void 0===o||o},t.prototype.allKnownElementNames=function(){return Object.keys(this.existingElements)},t.prototype.securityContext=function(t,o,r){return e.core.SecurityContext.NONE},t.prototype.getMappedPropName=function(t){return this.attrPropMapping[t]||t},t.prototype.getDefaultComponentElementName=function(){return"ng-component"},t.prototype.validateProperty=function(t){return this.invalidProperties.indexOf(t)>-1?{error:!0,msg:"Binding to property '"+t+"' is disallowed for security reasons"}:{error:!1}},t.prototype.validateAttribute=function(t){return this.invalidAttributes.indexOf(t)>-1?{error:!0,msg:"Binding to attribute '"+t+"' is disallowed for security reasons"}:{error:!1}},t.prototype.normalizeAnimationStyleProperty=function(t){return t},t.prototype.normalizeAnimationStyleValue=function(t,e,o){return{error:null,value:o.toString()}},t}(),u=function(t){function e(e){var o=t.call(this,e)||this;return o._directives=new Map,o}return r(e,t),e.prototype.resolve=function(e,o){return void 0===o&&(o=!0),this._directives.get(e)||t.prototype.resolve.call(this,e,o)},e.prototype.setDirective=function(t,e){this._directives.set(t,e)},e}(e.DirectiveResolver),c=function(t){function e(e){var o=t.call(this,e)||this;return o._ngModules=new Map,o}return r(e,t),e.prototype.setNgModule=function(t,e){this._ngModules.set(t,e)},e.prototype.resolve=function(e,o){return void 0===o&&(o=!0),this._ngModules.get(e)||t.prototype.resolve.call(this,e,o)},e}(e.NgModuleResolver),l=function(t){function e(e){var o=t.call(this,e)||this;return o._pipes=new Map,o}return r(e,t),e.prototype.setPipe=function(t,e){this._pipes.set(t,e)},e.prototype.resolve=function(e,o){void 0===o&&(o=!0);var r=this._pipes.get(e);return r||(r=t.prototype.resolve.call(this,e,o)),r},e}(e.PipeResolver);
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
t.MockResourceLoader=n,t.MockSchemaRegistry=p,t.MockDirectiveResolver=u,t.MockNgModuleResolver=c,t.MockPipeResolver=l,Object.defineProperty(t,"__esModule",{value:!0})});