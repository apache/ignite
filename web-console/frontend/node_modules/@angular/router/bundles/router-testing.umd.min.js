/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */
!function(e,r){"object"==typeof exports&&"undefined"!=typeof module?r(exports,require("@angular/common"),require("@angular/common/testing"),require("@angular/core"),require("@angular/router")):"function"==typeof define&&define.amd?define("@angular/router/testing",["exports","@angular/common","@angular/common/testing","@angular/core","@angular/router"],r):r(((e=e||self).ng=e.ng||{},e.ng.router=e.ng.router||{},e.ng.router.testing={}),e.ng.common,e.ng.common.testing,e.ng.core,e.ng.router)}(this,function(e,r,t,o,n){"use strict";function u(e,r,t,o){var n,u=arguments.length,a=u<3?r:null===o?o=Object.getOwnPropertyDescriptor(r,t):o;if("object"==typeof Reflect&&"function"==typeof Reflect.decorate)a=Reflect.decorate(e,r,t,o);else for(var i=e.length-1;i>=0;i--)(n=e[i])&&(a=(u<3?n(a):u>3?n(r,t,a):n(r,t))||a);return u>3&&a&&Object.defineProperty(r,t,a),a}function a(e,r){if("object"==typeof Reflect&&"function"==typeof Reflect.metadata)return Reflect.metadata(e,r)}
/**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
var i=function(){function e(e){this.compiler=e,this._stubbedModules={}}return Object.defineProperty(e.prototype,"stubbedModules",{get:function(){return this._stubbedModules},set:function(e){var r,t,o={};try{for(var n=function u(e){var r="function"==typeof Symbol&&e[Symbol.iterator],t=0;return r?r.call(e):{next:function(){return e&&t>=e.length&&(e=void 0),{value:e&&e[t++],done:!e}}}}(Object.keys(e)),a=n.next();!a.done;a=n.next()){var i=a.value;o[i]=this.compiler.compileModuleAsync(e[i])}}catch(e){r={error:e}}finally{try{a&&!a.done&&(t=n.return)&&t.call(n)}finally{if(r)throw r.error}}this._stubbedModules=o},enumerable:!0,configurable:!0}),e.prototype.load=function(e){return this._stubbedModules[e]?this._stubbedModules[e]:Promise.reject(new Error("Cannot find module "+e))},u([o.Injectable(),a("design:paramtypes",[o.Compiler])],e)}();function l(e,r,t,o,u,a,i,l,c){var d=new n.Router(null,e,r,t,a,o,u,n.ɵflatten(i));return l&&(function s(e){return"shouldProcessUrl"in e}(l)?d.urlHandlingStrategy=l:(l.malformedUriErrorHandler&&(d.malformedUriErrorHandler=l.malformedUriErrorHandler),l.paramsInheritanceStrategy&&(d.paramsInheritanceStrategy=l.paramsInheritanceStrategy))),c&&(d.urlHandlingStrategy=c),d}var c=function(){function e(){}var a;return a=e,e.withRoutes=function(e,r){return{ngModule:a,providers:[n.provideRoutes(e),{provide:n.ROUTER_CONFIGURATION,useValue:r||{}}]}},a=u([o.NgModule({exports:[n.RouterModule],providers:[n.ɵROUTER_PROVIDERS,{provide:r.Location,useClass:t.SpyLocation},{provide:r.LocationStrategy,useClass:t.MockLocationStrategy},{provide:o.NgModuleFactoryLoader,useClass:i},{provide:n.Router,useFactory:l,deps:[n.UrlSerializer,n.ChildrenOutletContexts,r.Location,o.NgModuleFactoryLoader,o.Compiler,o.Injector,n.ROUTES,n.ROUTER_CONFIGURATION,[n.UrlHandlingStrategy,new o.Optional]]},{provide:n.PreloadingStrategy,useExisting:n.NoPreloading},n.provideRoutes([])]})],e)}();
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
     */e.SpyNgModuleFactoryLoader=i,e.setupTestingRouter=l,e.RouterTestingModule=c,Object.defineProperty(e,"__esModule",{value:!0})});