/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */
!function(e,t){"object"==typeof exports&&"undefined"!=typeof module?t(exports,require("@angular/core"),require("@angular/platform-browser")):"function"==typeof define&&define.amd?define("@angular/platform-browser/testing",["exports","@angular/core","@angular/platform-browser"],t):t(((e=e||self).ng=e.ng||{},e.ng.platformBrowser=e.ng.platformBrowser||{},e.ng.platformBrowser.testing={}),e.ng.core,e.ng.platformBrowser)}(this,function(e,t,r){"use strict";var o=function(){function e(e){this._overrideUa=e}return Object.defineProperty(e.prototype,"_ua",{get:function(){return"string"==typeof this._overrideUa?this._overrideUa:r.ɵgetDOM()?r.ɵgetDOM().getUserAgent():""},enumerable:!0,configurable:!0}),e.setup=function(){new e(null)},Object.defineProperty(e.prototype,"isFirefox",{get:function(){return this._ua.indexOf("Firefox")>-1},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isAndroid",{get:function(){return this._ua.indexOf("Mozilla/5.0")>-1&&this._ua.indexOf("Android")>-1&&this._ua.indexOf("AppleWebKit")>-1&&-1==this._ua.indexOf("Chrome")&&-1==this._ua.indexOf("IEMobile")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isEdge",{get:function(){return this._ua.indexOf("Edge")>-1},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isIE",{get:function(){return this._ua.indexOf("Trident")>-1},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isWebkit",{get:function(){return this._ua.indexOf("AppleWebKit")>-1&&-1==this._ua.indexOf("Edge")&&-1==this._ua.indexOf("IEMobile")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isIOS7",{get:function(){return(this._ua.indexOf("iPhone OS 7")>-1||this._ua.indexOf("iPad OS 7")>-1)&&-1==this._ua.indexOf("IEMobile")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isSlow",{get:function(){return this.isAndroid||this.isIE||this.isIOS7},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsNativeIntlApi",{get:function(){return!!t.ɵglobal.Intl&&t.ɵglobal.Intl!==t.ɵglobal.IntlPolyfill},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isChromeDesktop",{get:function(){return this._ua.indexOf("Chrome")>-1&&-1==this._ua.indexOf("Mobile Safari")&&-1==this._ua.indexOf("Edge")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"isOldChrome",{get:function(){return this._ua.indexOf("Chrome")>-1&&this._ua.indexOf("Chrome/3")>-1&&-1==this._ua.indexOf("Edge")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsCustomElements",{get:function(){return void 0!==t.ɵglobal.customElements},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsDeprecatedCustomCustomElementsV0",{get:function(){return void 0!==document.registerElement},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsRegExUnicodeFlag",{get:function(){return RegExp.prototype.hasOwnProperty("unicode")},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsShadowDom",{get:function(){return void 0!==document.createElement("div").attachShadow},enumerable:!0,configurable:!0}),Object.defineProperty(e.prototype,"supportsDeprecatedShadowDomV0",{get:function(){return void 0!==document.createElement("div").createShadowRoot},enumerable:!0,configurable:!0}),e}();function n(){return new t.NgZone({enableLongStackTrace:!0})}o.setup();var i=t.createPlatformFactory(t.platformCore,"browserTesting",[{provide:t.PLATFORM_INITIALIZER,useValue:function u(){r.ɵBrowserDomAdapter.makeCurrent(),o.setup()},multi:!0}]),a=n,f=function(){return function o(e,t,r,n){var i,u=arguments.length,a=u<3?t:null===n?n=Object.getOwnPropertyDescriptor(t,r):n;if("object"==typeof Reflect&&"function"==typeof Reflect.decorate)a=Reflect.decorate(e,t,r,n);else for(var f=e.length-1;f>=0;f--)(i=e[f])&&(a=(u<3?i(a):u>3?i(t,r,a):i(t,r))||a);return u>3&&a&&Object.defineProperty(t,r,a),a}([t.NgModule({exports:[r.BrowserModule],providers:[{provide:t.APP_ID,useValue:"a"},r.ɵELEMENT_PROBE_PROVIDERS,{provide:t.NgZone,useFactory:a}]})],function e(){})}();
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
e.ɵangular_packages_platform_browser_testing_testing_a=n,e.platformBrowserTesting=i,e.BrowserTestingModule=f,e.ɵ0=a,Object.defineProperty(e,"__esModule",{value:!0})});