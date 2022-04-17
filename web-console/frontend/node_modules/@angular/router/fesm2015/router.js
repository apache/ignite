/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { LocationStrategy, Location, PlatformLocation, APP_BASE_HREF, ViewportScroller, HashLocationStrategy, PathLocationStrategy, LOCATION_INITIALIZED } from '@angular/common';
import { Component, ɵisObservable, ɵisPromise, NgModuleRef, InjectionToken, NgModuleFactory, ɵConsole, NgZone, isDevMode, Directive, Attribute, Renderer2, ElementRef, Input, HostListener, HostBinding, Optional, ContentChildren, EventEmitter, ViewContainerRef, ComponentFactoryResolver, ChangeDetectorRef, Output, Injectable, NgModuleFactoryLoader, Compiler, Injector, ApplicationRef, SystemJsNgModuleLoader, NgProbeToken, ANALYZE_FOR_ENTRY_COMPONENTS, SkipSelf, Inject, APP_INITIALIZER, APP_BOOTSTRAP_LISTENER, NgModule, Version } from '@angular/core';
import { of, from, BehaviorSubject, Observable, EmptyError, combineLatest, defer, Subject, EMPTY } from 'rxjs';
import { map, concatAll, last as last$1, catchError, first, mergeMap, every, switchMap, take, startWith, scan, filter, concatMap, reduce, tap, finalize, mergeAll } from 'rxjs/operators';
import { ɵgetDOM } from '@angular/platform-browser';

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
/**
 * Base for events the router goes through, as opposed to events tied to a specific
 * route. Fired one time for any given navigation.
 *
 * \@usageNotes
 *
 * ```ts
 * class MyService {
 *   constructor(public router: Router, logger: Logger) {
 *     router.events.pipe(
 *       filter(e => e instanceof RouterEvent)
 *     ).subscribe(e => {
 *       logger.log(e.id, e.url);
 *     });
 *   }
 * }
 * ```
 *
 * @see `Event`
 * \@publicApi
 */
class RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     */
    constructor(id, url) {
        this.id = id;
        this.url = url;
    }
}
if (false) {
    /**
     * A unique ID that the router assigns to every router navigation.
     * @type {?}
     */
    RouterEvent.prototype.id;
    /**
     * The URL that is the destination for this navigation.
     * @type {?}
     */
    RouterEvent.prototype.url;
}
/**
 * An event triggered when a navigation starts.
 *
 * \@publicApi
 */
class NavigationStart extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?=} navigationTrigger
     * @param {?=} restoredState
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    navigationTrigger = 'imperative', 
    /** @docsNotRequired */
    restoredState = null) {
        super(id, url);
        this.navigationTrigger = navigationTrigger;
        this.restoredState = restoredState;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return `NavigationStart(id: ${this.id}, url: '${this.url}')`; }
}
if (false) {
    /**
     * Identifies the call or event that triggered the navigation.
     * An `imperative` trigger is a call to `router.navigateByUrl()` or `router.navigate()`.
     *
     * @type {?}
     */
    NavigationStart.prototype.navigationTrigger;
    /**
     * The navigation state that was previously supplied to the `pushState` call,
     * when the navigation is triggered by a `popstate` event. Otherwise null.
     *
     * The state object is defined by `NavigationExtras`, and contains any
     * developer-defined state value, as well as a unique ID that
     * the router assigns to every router transition/navigation.
     *
     * From the perspective of the router, the router never "goes back".
     * When the user clicks on the back button in the browser,
     * a new navigation ID is created.
     *
     * Use the ID in this previous-state object to differentiate between a newly created
     * state and one returned to by a `popstate` event, so that you can restore some
     * remembered state, such as scroll position.
     *
     * @type {?}
     */
    NavigationStart.prototype.restoredState;
}
/**
 * An event triggered when a navigation ends successfully.
 *
 * \@publicApi
 */
class NavigationEnd extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() {
        return `NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    NavigationEnd.prototype.urlAfterRedirects;
}
/**
 * An event triggered when a navigation is canceled, directly or indirectly.
 *
 * This can happen when a [route guard](guide/router#milestone-5-route-guards)
 * returns `false` or initiates a redirect by returning a `UrlTree`.
 *
 * \@publicApi
 */
class NavigationCancel extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} reason
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, reason) {
        super(id, url);
        this.reason = reason;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return `NavigationCancel(id: ${this.id}, url: '${this.url}')`; }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    NavigationCancel.prototype.reason;
}
/**
 * An event triggered when a navigation fails due to an unexpected error.
 *
 * \@publicApi
 */
class NavigationError extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} error
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, error) {
        super(id, url);
        this.error = error;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() {
        return `NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    NavigationError.prototype.error;
}
/**
 * An event triggered when routes are recognized.
 *
 * \@publicApi
 */
class RoutesRecognized extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     * @param {?} state
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects, state) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
        this.state = state;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() {
        return `RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    RoutesRecognized.prototype.urlAfterRedirects;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    RoutesRecognized.prototype.state;
}
/**
 * An event triggered at the start of the Guard phase of routing.
 *
 * \@publicApi
 */
class GuardsCheckStart extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     * @param {?} state
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects, state) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
        this.state = state;
    }
    /**
     * @return {?}
     */
    toString() {
        return `GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    GuardsCheckStart.prototype.urlAfterRedirects;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    GuardsCheckStart.prototype.state;
}
/**
 * An event triggered at the end of the Guard phase of routing.
 *
 * \@publicApi
 */
class GuardsCheckEnd extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     * @param {?} state
     * @param {?} shouldActivate
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects, state, shouldActivate) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
        this.state = state;
        this.shouldActivate = shouldActivate;
    }
    /**
     * @return {?}
     */
    toString() {
        return `GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    GuardsCheckEnd.prototype.urlAfterRedirects;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    GuardsCheckEnd.prototype.state;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    GuardsCheckEnd.prototype.shouldActivate;
}
/**
 * An event triggered at the the start of the Resolve phase of routing.
 *
 * Runs in the "resolve" phase whether or not there is anything to resolve.
 * In future, may change to only run when there are things to be resolved.
 *
 * \@publicApi
 */
class ResolveStart extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     * @param {?} state
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects, state) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
        this.state = state;
    }
    /**
     * @return {?}
     */
    toString() {
        return `ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ResolveStart.prototype.urlAfterRedirects;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ResolveStart.prototype.state;
}
/**
 * An event triggered at the end of the Resolve phase of routing.
 * @see `ResolveStart`.
 *
 * \@publicApi
 */
class ResolveEnd extends RouterEvent {
    /**
     * @param {?} id
     * @param {?} url
     * @param {?} urlAfterRedirects
     * @param {?} state
     */
    constructor(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, urlAfterRedirects, state) {
        super(id, url);
        this.urlAfterRedirects = urlAfterRedirects;
        this.state = state;
    }
    /**
     * @return {?}
     */
    toString() {
        return `ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ResolveEnd.prototype.urlAfterRedirects;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ResolveEnd.prototype.state;
}
/**
 * An event triggered before lazy loading a route configuration.
 *
 * \@publicApi
 */
class RouteConfigLoadStart {
    /**
     * @param {?} route
     */
    constructor(route) {
        this.route = route;
    }
    /**
     * @return {?}
     */
    toString() { return `RouteConfigLoadStart(path: ${this.route.path})`; }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    RouteConfigLoadStart.prototype.route;
}
/**
 * An event triggered when a route has been lazy loaded.
 *
 * \@publicApi
 */
class RouteConfigLoadEnd {
    /**
     * @param {?} route
     */
    constructor(route) {
        this.route = route;
    }
    /**
     * @return {?}
     */
    toString() { return `RouteConfigLoadEnd(path: ${this.route.path})`; }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    RouteConfigLoadEnd.prototype.route;
}
/**
 * An event triggered at the start of the child-activation
 * part of the Resolve phase of routing.
 * @see `ChildActivationEnd`
 * @see `ResolveStart`
 *
 * \@publicApi
 */
class ChildActivationStart {
    /**
     * @param {?} snapshot
     */
    constructor(snapshot) {
        this.snapshot = snapshot;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return `ChildActivationStart(path: '${path}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ChildActivationStart.prototype.snapshot;
}
/**
 * An event triggered at the end of the child-activation part
 * of the Resolve phase of routing.
 * @see `ChildActivationStart`
 * @see `ResolveStart` *
 * \@publicApi
 */
class ChildActivationEnd {
    /**
     * @param {?} snapshot
     */
    constructor(snapshot) {
        this.snapshot = snapshot;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return `ChildActivationEnd(path: '${path}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ChildActivationEnd.prototype.snapshot;
}
/**
 * An event triggered at the start of the activation part
 * of the Resolve phase of routing.
 * @see ActivationEnd`
 * @see `ResolveStart`
 *
 * \@publicApi
 */
class ActivationStart {
    /**
     * @param {?} snapshot
     */
    constructor(snapshot) {
        this.snapshot = snapshot;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return `ActivationStart(path: '${path}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ActivationStart.prototype.snapshot;
}
/**
 * An event triggered at the end of the activation part
 * of the Resolve phase of routing.
 * @see `ActivationStart`
 * @see `ResolveStart`
 *
 * \@publicApi
 */
class ActivationEnd {
    /**
     * @param {?} snapshot
     */
    constructor(snapshot) {
        this.snapshot = snapshot;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return `ActivationEnd(path: '${path}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    ActivationEnd.prototype.snapshot;
}
/**
 * An event triggered by scrolling.
 *
 * \@publicApi
 */
class Scroll {
    /**
     * @param {?} routerEvent
     * @param {?} position
     * @param {?} anchor
     */
    constructor(routerEvent, position, anchor) {
        this.routerEvent = routerEvent;
        this.position = position;
        this.anchor = anchor;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const pos = this.position ? `${this.position[0]}, ${this.position[1]}` : null;
        return `Scroll(anchor: '${this.anchor}', position: '${pos}')`;
    }
}
if (false) {
    /**
     * \@docsNotRequired
     * @type {?}
     */
    Scroll.prototype.routerEvent;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    Scroll.prototype.position;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    Scroll.prototype.anchor;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * This component is used internally within the router to be a placeholder when an empty
 * router-outlet is needed. For example, with a config such as:
 *
 * `{path: 'parent', outlet: 'nav', children: [...]}`
 *
 * In order to render, there needs to be a component on this config, which will default
 * to this `EmptyOutletComponent`.
 */
class ɵEmptyOutletComponent {
}
ɵEmptyOutletComponent.decorators = [
    { type: Component, args: [{ template: `<router-outlet></router-outlet>` }] }
];

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
/**
 * The primary routing outlet.
 *
 * \@publicApi
 * @type {?}
 */
const PRIMARY_OUTLET = 'primary';
/**
 * A map that provides access to the required and optional parameters
 * specific to a route.
 * The map supports retrieving a single value with `get()`
 * or multiple values with `getAll()`.
 *
 * @see [URLSearchParams](https://developer.mozilla.org/en-US/docs/Web/API/URLSearchParams)
 *
 * \@publicApi
 * @record
 */
function ParamMap() { }
if (false) {
    /**
     * Names of the parameters in the map.
     * @type {?}
     */
    ParamMap.prototype.keys;
    /**
     * Reports whether the map contains a given parameter.
     * @param {?} name The parameter name.
     * @return {?} True if the map contains the given parameter, false otherwise.
     */
    ParamMap.prototype.has = function (name) { };
    /**
     * Retrieves a single value for a parameter.
     * @param {?} name The parameter name.
     * @return {?} The parameter's single value,
     * or the first value if the parameter has multiple values,
     * or `null` when there is no such parameter.
     */
    ParamMap.prototype.get = function (name) { };
    /**
     * Retrieves multiple values for a parameter.
     * @param {?} name The parameter name.
     * @return {?} An array containing one or more values,
     * or an empty array if there is no such parameter.
     *
     */
    ParamMap.prototype.getAll = function (name) { };
}
class ParamsAsMap {
    /**
     * @param {?} params
     */
    constructor(params) { this.params = params || {}; }
    /**
     * @param {?} name
     * @return {?}
     */
    has(name) { return this.params.hasOwnProperty(name); }
    /**
     * @param {?} name
     * @return {?}
     */
    get(name) {
        if (this.has(name)) {
            /** @type {?} */
            const v = this.params[name];
            return Array.isArray(v) ? v[0] : v;
        }
        return null;
    }
    /**
     * @param {?} name
     * @return {?}
     */
    getAll(name) {
        if (this.has(name)) {
            /** @type {?} */
            const v = this.params[name];
            return Array.isArray(v) ? v : [v];
        }
        return [];
    }
    /**
     * @return {?}
     */
    get keys() { return Object.keys(this.params); }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ParamsAsMap.prototype.params;
}
/**
 * Converts a `Params` instance to a `ParamMap`.
 * \@publicApi
 * @param {?} params The instance to convert.
 * @return {?} The new map instance.
 *
 */
function convertToParamMap(params) {
    return new ParamsAsMap(params);
}
/** @type {?} */
const NAVIGATION_CANCELING_ERROR = 'ngNavigationCancelingError';
/**
 * @param {?} message
 * @return {?}
 */
function navigationCancelingError(message) {
    /** @type {?} */
    const error = Error('NavigationCancelingError: ' + message);
    ((/** @type {?} */ (error)))[NAVIGATION_CANCELING_ERROR] = true;
    return error;
}
/**
 * @param {?} error
 * @return {?}
 */
function isNavigationCancelingError(error) {
    return error && ((/** @type {?} */ (error)))[NAVIGATION_CANCELING_ERROR];
}
// Matches the route configuration (`route`) against the actual URL (`segments`).
/**
 * @param {?} segments
 * @param {?} segmentGroup
 * @param {?} route
 * @return {?}
 */
function defaultUrlMatcher(segments, segmentGroup, route) {
    /** @type {?} */
    const parts = (/** @type {?} */ (route.path)).split('/');
    if (parts.length > segments.length) {
        // The actual URL is shorter than the config, no match
        return null;
    }
    if (route.pathMatch === 'full' &&
        (segmentGroup.hasChildren() || parts.length < segments.length)) {
        // The config is longer than the actual URL but we are looking for a full match, return null
        return null;
    }
    /** @type {?} */
    const posParams = {};
    // Check each config part against the actual URL
    for (let index = 0; index < parts.length; index++) {
        /** @type {?} */
        const part = parts[index];
        /** @type {?} */
        const segment = segments[index];
        /** @type {?} */
        const isParameter = part.startsWith(':');
        if (isParameter) {
            posParams[part.substring(1)] = segment;
        }
        else if (part !== segment.path) {
            // The actual URL part does not match the config, no match
            return null;
        }
    }
    return { consumed: segments.slice(0, parts.length), posParams };
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A configuration object that defines a single route.
 * A set of routes are collected in a `Routes` array to define a `Router` configuration.
 * The router attempts to match segments of a given URL against each route,
 * using the configuration options defined in this object.
 *
 * Supports static, parameterized, redirect, and wildcard routes, as well as
 * custom route data and resolve methods.
 *
 * For detailed usage information, see the [Routing Guide](guide/router).
 *
 * \@usageNotes
 *
 * ### Simple Configuration
 *
 * The following route specifies that when navigating to, for example,
 * `/team/11/user/bob`, the router creates the 'Team' component
 * with the 'User' child component in it.
 *
 * ```
 * [{
 *   path: 'team/:id',
 *  component: Team,
 *   children: [{
 *     path: 'user/:name',
 *     component: User
 *   }]
 * }]
 * ```
 *
 * ### Multiple Outlets
 *
 * The following route creates sibling components with multiple outlets.
 * When navigating to `/team/11(aux:chat/jim)`, the router creates the 'Team' component next to
 * the 'Chat' component. The 'Chat' component is placed into the 'aux' outlet.
 *
 * ```
 * [{
 *   path: 'team/:id',
 *   component: Team
 * }, {
 *   path: 'chat/:user',
 *   component: Chat
 *   outlet: 'aux'
 * }]
 * ```
 *
 * ### Wild Cards
 *
 * The following route uses wild-card notation to specify a component
 * that is always instantiated regardless of where you navigate to.
 *
 * ```
 * [{
 *   path: '**',
 *   component: WildcardComponent
 * }]
 * ```
 *
 * ### Redirects
 *
 * The following route uses the `redirectTo` property to ignore a segment of
 * a given URL when looking for a child path.
 *
 * When navigating to '/team/11/legacy/user/jim', the router changes the URL segment
 * '/team/11/legacy/user/jim' to '/team/11/user/jim', and then instantiates
 * the Team component with the User child component in it.
 *
 * ```
 * [{
 *   path: 'team/:id',
 *   component: Team,
 *   children: [{
 *     path: 'legacy/user/:name',
 *     redirectTo: 'user/:name'
 *   }, {
 *     path: 'user/:name',
 *     component: User
 *   }]
 * }]
 * ```
 *
 * The redirect path can be relative, as shown in this example, or absolute.
 * If we change the `redirectTo` value in the example to the absolute URL segment '/user/:name',
 * the result URL is also absolute, '/user/jim'.
 * ### Empty Path
 *
 * Empty-path route configurations can be used to instantiate components that do not 'consume'
 * any URL segments.
 *
 * In the following configuration, when navigating to
 * `/team/11`, the router instantiates the 'AllUsers' component.
 *
 * ```
 * [{
 *   path: 'team/:id',
 *   component: Team,
 *   children: [{
 *     path: '',
 *     component: AllUsers
 *   }, {
 *     path: 'user/:name',
 *     component: User
 *   }]
 * }]
 * ```
 *
 * Empty-path routes can have children. In the following example, when navigating
 * to `/team/11/user/jim`, the router instantiates the wrapper component with
 * the user component in it.
 *
 * Note that an empty path route inherits its parent's parameters and data.
 *
 * ```
 * [{
 *   path: 'team/:id',
 *   component: Team,
 *   children: [{
 *     path: '',
 *     component: WrapperCmp,
 *     children: [{
 *       path: 'user/:name',
 *       component: User
 *     }]
 *   }]
 * }]
 * ```
 *
 * ### Matching Strategy
 *
 * The default path-match strategy is 'prefix', which means that the router
 * checks URL elements from the left to see if the URL matches a specified path.
 * For example, '/team/11/user' matches 'team/:id'.
 *
 * ```
 * [{
 *   path: '',
 *   pathMatch: 'prefix', //default
 *   redirectTo: 'main'
 * }, {
 *   path: 'main',
 *   component: Main
 * }]
 * ```
 *
 * You can specify the path-match strategy 'full' to make sure that the path
 * covers the whole unconsumed URL. It is important to do this when redirecting
 * empty-path routes. Otherwise, because an empty path is a prefix of any URL,
 * the router would apply the redirect even when navigating to the redirect destination,
 * creating an endless loop.
 *
 * In the following example, supplying the 'full' `pathMatch` strategy ensures
 * that the router applies the redirect if and only if navigating to '/'.
 *
 * ```
 * [{
 *   path: '',
 *   pathMatch: 'full',
 *   redirectTo: 'main'
 * }, {
 *   path: 'main',
 *   component: Main
 * }]
 * ```
 *
 * ### Componentless Routes
 *
 * You can share parameters between sibling components.
 * For example, suppose that two sibling components should go next to each other,
 * and both of them require an ID parameter. You can accomplish this using a route
 * that does not specify a component at the top level.
 *
 * In the following example, 'MainChild' and 'AuxChild' are siblings.
 * When navigating to 'parent/10/(a//aux:b)', the route instantiates
 * the main child and aux child components next to each other.
 * For this to work, the application component must have the primary and aux outlets defined.
 *
 * ```
 * [{
 *    path: 'parent/:id',
 *    children: [
 *      { path: 'a', component: MainChild },
 *      { path: 'b', component: AuxChild, outlet: 'aux' }
 *    ]
 * }]
 * ```
 *
 * The router merges the parameters, data, and resolve of the componentless
 * parent into the parameters, data, and resolve of the children.
 *
 * This is especially useful when child components are defined
 * with an empty path string, as in the following example.
 * With this configuration, navigating to '/parent/10' creates
 * the main child and aux components.
 *
 * ```
 * [{
 *    path: 'parent/:id',
 *    children: [
 *      { path: '', component: MainChild },
 *      { path: '', component: AuxChild, outlet: 'aux' }
 *    ]
 * }]
 * ```
 *
 * ### Lazy Loading
 *
 * Lazy loading speeds up application load time by splitting the application
 * into multiple bundles and loading them on demand.
 * To use lazy loading, provide the `loadChildren` property  instead of the `children` property.
 *
 * Given the following example route, the router will lazy load
 * the associated module on demand using the browser native import system.
 *
 * ```
 * [{
 *   path: 'lazy',
 *   loadChildren: () => import('./lazy-route/lazy.module').then(mod => mod.LazyModule),
 * }];
 * ```
 *
 * \@publicApi
 * @record
 */
function Route() { }
if (false) {
    /**
     * The path to match against. Cannot be used together with a custom `matcher` function.
     * A URL string that uses router matching notation.
     * Can be a wild card (`**`) that matches any URL (see Usage Notes below).
     * Default is "/" (the root path).
     *
     * @type {?|undefined}
     */
    Route.prototype.path;
    /**
     * The path-matching strategy, one of 'prefix' or 'full'.
     * Default is 'prefix'.
     *
     * By default, the router checks URL elements from the left to see if the URL
     * matches a given  path, and stops when there is a match. For example,
     * '/team/11/user' matches 'team/:id'.
     *
     * The path-match strategy 'full' matches against the entire URL.
     * It is important to do this when redirecting empty-path routes.
     * Otherwise, because an empty path is a prefix of any URL,
     * the router would apply the redirect even when navigating
     * to the redirect destination, creating an endless loop.
     *
     * @type {?|undefined}
     */
    Route.prototype.pathMatch;
    /**
     * A custom URL-matching function. Cannot be used together with `path`.
     * @type {?|undefined}
     */
    Route.prototype.matcher;
    /**
     * The component to instantiate when the path matches.
     * Can be empty if child routes specify components.
     * @type {?|undefined}
     */
    Route.prototype.component;
    /**
     * A URL to which to redirect when a the path matches.
     * Absolute if the URL begins with a slash (/), otherwise relative to the path URL.
     * When not present, router does not redirect.
     * @type {?|undefined}
     */
    Route.prototype.redirectTo;
    /**
     * Name of a `RouterOutlet` object where the component can be placed
     * when the path matches.
     * @type {?|undefined}
     */
    Route.prototype.outlet;
    /**
     * An array of dependency-injection tokens used to look up `CanActivate()`
     * handlers, in order to determine if the current user is allowed to
     * activate the component. By default, any user can activate.
     * @type {?|undefined}
     */
    Route.prototype.canActivate;
    /**
     * An array of DI tokens used to look up `CanActivateChild()` handlers,
     * in order to determine if the current user is allowed to activate
     * a child of the component. By default, any user can activate a child.
     * @type {?|undefined}
     */
    Route.prototype.canActivateChild;
    /**
     * An array of DI tokens used to look up `CanDeactivate()`
     * handlers, in order to determine if the current user is allowed to
     * deactivate the component. By default, any user can deactivate.
     *
     * @type {?|undefined}
     */
    Route.prototype.canDeactivate;
    /**
     * An array of DI tokens used to look up `CanLoad()`
     * handlers, in order to determine if the current user is allowed to
     * load the component. By default, any user can load.
     * @type {?|undefined}
     */
    Route.prototype.canLoad;
    /**
     * Additional developer-defined data provided to the component via
     * `ActivatedRoute`. By default, no additional data is passed.
     * @type {?|undefined}
     */
    Route.prototype.data;
    /**
     * A map of DI tokens used to look up data resolvers. See `Resolve`.
     * @type {?|undefined}
     */
    Route.prototype.resolve;
    /**
     * An array of child `Route` objects that specifies a nested route
     * configuration.
     * @type {?|undefined}
     */
    Route.prototype.children;
    /**
     * A `LoadChildren` object specifying lazy-loaded child routes.
     * @type {?|undefined}
     */
    Route.prototype.loadChildren;
    /**
     * Defines when guards and resolvers will be run. One of
     * - `paramsOrQueryParamsChange` : Run when query parameters change.
     * - `always` : Run on every execution.
     * By default, guards and resolvers run only when the matrix
     * parameters of the route change.
     * @type {?|undefined}
     */
    Route.prototype.runGuardsAndResolvers;
    /**
     * Filled for routes with `loadChildren` once the module has been loaded
     * \@internal
     * @type {?|undefined}
     */
    Route.prototype._loadedConfig;
}
class LoadedRouterConfig {
    /**
     * @param {?} routes
     * @param {?} module
     */
    constructor(routes, module) {
        this.routes = routes;
        this.module = module;
    }
}
if (false) {
    /** @type {?} */
    LoadedRouterConfig.prototype.routes;
    /** @type {?} */
    LoadedRouterConfig.prototype.module;
}
/**
 * @param {?} config
 * @param {?=} parentPath
 * @return {?}
 */
function validateConfig(config, parentPath = '') {
    // forEach doesn't iterate undefined values
    for (let i = 0; i < config.length; i++) {
        /** @type {?} */
        const route = config[i];
        /** @type {?} */
        const fullPath = getFullPath(parentPath, route);
        validateNode(route, fullPath);
    }
}
/**
 * @param {?} route
 * @param {?} fullPath
 * @return {?}
 */
function validateNode(route, fullPath) {
    if (!route) {
        throw new Error(`
      Invalid configuration of route '${fullPath}': Encountered undefined route.
      The reason might be an extra comma.

      Example:
      const routes: Routes = [
        { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
        { path: 'dashboard',  component: DashboardComponent },, << two commas
        { path: 'detail/:id', component: HeroDetailComponent }
      ];
    `);
    }
    if (Array.isArray(route)) {
        throw new Error(`Invalid configuration of route '${fullPath}': Array cannot be specified`);
    }
    if (!route.component && !route.children && !route.loadChildren &&
        (route.outlet && route.outlet !== PRIMARY_OUTLET)) {
        throw new Error(`Invalid configuration of route '${fullPath}': a componentless route without children or loadChildren cannot have a named outlet set`);
    }
    if (route.redirectTo && route.children) {
        throw new Error(`Invalid configuration of route '${fullPath}': redirectTo and children cannot be used together`);
    }
    if (route.redirectTo && route.loadChildren) {
        throw new Error(`Invalid configuration of route '${fullPath}': redirectTo and loadChildren cannot be used together`);
    }
    if (route.children && route.loadChildren) {
        throw new Error(`Invalid configuration of route '${fullPath}': children and loadChildren cannot be used together`);
    }
    if (route.redirectTo && route.component) {
        throw new Error(`Invalid configuration of route '${fullPath}': redirectTo and component cannot be used together`);
    }
    if (route.path && route.matcher) {
        throw new Error(`Invalid configuration of route '${fullPath}': path and matcher cannot be used together`);
    }
    if (route.redirectTo === void 0 && !route.component && !route.children && !route.loadChildren) {
        throw new Error(`Invalid configuration of route '${fullPath}'. One of the following must be provided: component, redirectTo, children or loadChildren`);
    }
    if (route.path === void 0 && route.matcher === void 0) {
        throw new Error(`Invalid configuration of route '${fullPath}': routes must have either a path or a matcher specified`);
    }
    if (typeof route.path === 'string' && route.path.charAt(0) === '/') {
        throw new Error(`Invalid configuration of route '${fullPath}': path cannot start with a slash`);
    }
    if (route.path === '' && route.redirectTo !== void 0 && route.pathMatch === void 0) {
        /** @type {?} */
        const exp = `The default value of 'pathMatch' is 'prefix', but often the intent is to use 'full'.`;
        throw new Error(`Invalid configuration of route '{path: "${fullPath}", redirectTo: "${route.redirectTo}"}': please provide 'pathMatch'. ${exp}`);
    }
    if (route.pathMatch !== void 0 && route.pathMatch !== 'full' && route.pathMatch !== 'prefix') {
        throw new Error(`Invalid configuration of route '${fullPath}': pathMatch can only be set to 'prefix' or 'full'`);
    }
    if (route.children) {
        validateConfig(route.children, fullPath);
    }
}
/**
 * @param {?} parentPath
 * @param {?} currentRoute
 * @return {?}
 */
function getFullPath(parentPath, currentRoute) {
    if (!currentRoute) {
        return parentPath;
    }
    if (!parentPath && !currentRoute.path) {
        return '';
    }
    else if (parentPath && !currentRoute.path) {
        return `${parentPath}/`;
    }
    else if (!parentPath && currentRoute.path) {
        return currentRoute.path;
    }
    else {
        return `${parentPath}/${currentRoute.path}`;
    }
}
/**
 * Makes a copy of the config and adds any default required properties.
 * @param {?} r
 * @return {?}
 */
function standardizeConfig(r) {
    /** @type {?} */
    const children = r.children && r.children.map(standardizeConfig);
    /** @type {?} */
    const c = children ? Object.assign({}, r, { children }) : Object.assign({}, r);
    if (!c.component && (children || c.loadChildren) && (c.outlet && c.outlet !== PRIMARY_OUTLET)) {
        c.component = ɵEmptyOutletComponent;
    }
    return c;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
function shallowEqualArrays(a, b) {
    if (a.length !== b.length)
        return false;
    for (let i = 0; i < a.length; ++i) {
        if (!shallowEqual(a[i], b[i]))
            return false;
    }
    return true;
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
function shallowEqual(a, b) {
    // Casting Object.keys return values to include `undefined` as there are some cases
    // in IE 11 where this can happen. Cannot provide a test because the behavior only
    // exists in certain circumstances in IE 11, therefore doing this cast ensures the
    // logic is correct for when this edge case is hit.
    /** @type {?} */
    const k1 = (/** @type {?} */ (Object.keys(a)));
    /** @type {?} */
    const k2 = (/** @type {?} */ (Object.keys(b)));
    if (!k1 || !k2 || k1.length != k2.length) {
        return false;
    }
    /** @type {?} */
    let key;
    for (let i = 0; i < k1.length; i++) {
        key = k1[i];
        if (a[key] !== b[key]) {
            return false;
        }
    }
    return true;
}
/**
 * Flattens single-level nested arrays.
 * @template T
 * @param {?} arr
 * @return {?}
 */
function flatten(arr) {
    return Array.prototype.concat.apply([], arr);
}
/**
 * Return the last element of an array.
 * @template T
 * @param {?} a
 * @return {?}
 */
function last(a) {
    return a.length > 0 ? a[a.length - 1] : null;
}
/**
 * Verifys all booleans in an array are `true`.
 * @param {?} bools
 * @return {?}
 */
function and(bools) {
    return !bools.some((/**
     * @param {?} v
     * @return {?}
     */
    v => !v));
}
/**
 * @template K, V
 * @param {?} map
 * @param {?} callback
 * @return {?}
 */
function forEach(map, callback) {
    for (const prop in map) {
        if (map.hasOwnProperty(prop)) {
            callback(map[prop], prop);
        }
    }
}
/**
 * @template A, B
 * @param {?} obj
 * @param {?} fn
 * @return {?}
 */
function waitForMap(obj, fn) {
    if (Object.keys(obj).length === 0) {
        return of({});
    }
    /** @type {?} */
    const waitHead = [];
    /** @type {?} */
    const waitTail = [];
    /** @type {?} */
    const res = {};
    forEach(obj, (/**
     * @param {?} a
     * @param {?} k
     * @return {?}
     */
    (a, k) => {
        /** @type {?} */
        const mapped = fn(k, a).pipe(map((/**
         * @param {?} r
         * @return {?}
         */
        (r) => res[k] = r)));
        if (k === PRIMARY_OUTLET) {
            waitHead.push(mapped);
        }
        else {
            waitTail.push(mapped);
        }
    }));
    // Closure compiler has problem with using spread operator here. So we use "Array.concat".
    // Note that we also need to cast the new promise because TypeScript cannot infer the type
    // when calling the "of" function through "Function.apply"
    return ((/** @type {?} */ (of.apply(null, waitHead.concat(waitTail)))))
        .pipe(concatAll(), last$1(), map((/**
     * @return {?}
     */
    () => res)));
}
/**
 * @template T
 * @param {?} value
 * @return {?}
 */
function wrapIntoObservable(value) {
    if (ɵisObservable(value)) {
        return value;
    }
    if (ɵisPromise(value)) {
        // Use `Promise.resolve()` to wrap promise-like instances.
        // Required ie when a Resolver returns a AngularJS `$q` promise to correctly trigger the
        // change detection.
        return from(Promise.resolve(value));
    }
    return of(value);
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @return {?}
 */
function createEmptyUrlTree() {
    return new UrlTree(new UrlSegmentGroup([], {}), {}, null);
}
/**
 * @param {?} container
 * @param {?} containee
 * @param {?} exact
 * @return {?}
 */
function containsTree(container, containee, exact) {
    if (exact) {
        return equalQueryParams(container.queryParams, containee.queryParams) &&
            equalSegmentGroups(container.root, containee.root);
    }
    return containsQueryParams(container.queryParams, containee.queryParams) &&
        containsSegmentGroup(container.root, containee.root);
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function equalQueryParams(container, containee) {
    // TODO: This does not handle array params correctly.
    return shallowEqual(container, containee);
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function equalSegmentGroups(container, containee) {
    if (!equalPath(container.segments, containee.segments))
        return false;
    if (container.numberOfChildren !== containee.numberOfChildren)
        return false;
    for (const c in containee.children) {
        if (!container.children[c])
            return false;
        if (!equalSegmentGroups(container.children[c], containee.children[c]))
            return false;
    }
    return true;
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function containsQueryParams(container, containee) {
    // TODO: This does not handle array params correctly.
    return Object.keys(containee).length <= Object.keys(container).length &&
        Object.keys(containee).every((/**
         * @param {?} key
         * @return {?}
         */
        key => containee[key] === container[key]));
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function containsSegmentGroup(container, containee) {
    return containsSegmentGroupHelper(container, containee, containee.segments);
}
/**
 * @param {?} container
 * @param {?} containee
 * @param {?} containeePaths
 * @return {?}
 */
function containsSegmentGroupHelper(container, containee, containeePaths) {
    if (container.segments.length > containeePaths.length) {
        /** @type {?} */
        const current = container.segments.slice(0, containeePaths.length);
        if (!equalPath(current, containeePaths))
            return false;
        if (containee.hasChildren())
            return false;
        return true;
    }
    else if (container.segments.length === containeePaths.length) {
        if (!equalPath(container.segments, containeePaths))
            return false;
        for (const c in containee.children) {
            if (!container.children[c])
                return false;
            if (!containsSegmentGroup(container.children[c], containee.children[c]))
                return false;
        }
        return true;
    }
    else {
        /** @type {?} */
        const current = containeePaths.slice(0, container.segments.length);
        /** @type {?} */
        const next = containeePaths.slice(container.segments.length);
        if (!equalPath(container.segments, current))
            return false;
        if (!container.children[PRIMARY_OUTLET])
            return false;
        return containsSegmentGroupHelper(container.children[PRIMARY_OUTLET], containee, next);
    }
}
/**
 * \@description
 *
 * Represents the parsed URL.
 *
 * Since a router state is a tree, and the URL is nothing but a serialized state, the URL is a
 * serialized tree.
 * UrlTree is a data structure that provides a lot of affordances in dealing with URLs
 *
 * \@usageNotes
 * ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const tree: UrlTree =
 *       router.parseUrl('/team/33/(user/victor//support:help)?debug=true#fragment');
 *     const f = tree.fragment; // return 'fragment'
 *     const q = tree.queryParams; // returns {debug: 'true'}
 *     const g: UrlSegmentGroup = tree.root.children[PRIMARY_OUTLET];
 *     const s: UrlSegment[] = g.segments; // returns 2 segments 'team' and '33'
 *     g.children[PRIMARY_OUTLET].segments; // returns 2 segments 'user' and 'victor'
 *     g.children['support'].segments; // return 1 segment 'help'
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
class UrlTree {
    /**
     * \@internal
     * @param {?} root
     * @param {?} queryParams
     * @param {?} fragment
     */
    constructor(root, queryParams, fragment) {
        this.root = root;
        this.queryParams = queryParams;
        this.fragment = fragment;
    }
    /**
     * @return {?}
     */
    get queryParamMap() {
        if (!this._queryParamMap) {
            this._queryParamMap = convertToParamMap(this.queryParams);
        }
        return this._queryParamMap;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return DEFAULT_SERIALIZER.serialize(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlTree.prototype._queryParamMap;
    /**
     * The root segment group of the URL tree
     * @type {?}
     */
    UrlTree.prototype.root;
    /**
     * The query params of the URL
     * @type {?}
     */
    UrlTree.prototype.queryParams;
    /**
     * The fragment of the URL
     * @type {?}
     */
    UrlTree.prototype.fragment;
}
/**
 * \@description
 *
 * Represents the parsed URL segment group.
 *
 * See `UrlTree` for more information.
 *
 * \@publicApi
 */
class UrlSegmentGroup {
    /**
     * @param {?} segments
     * @param {?} children
     */
    constructor(segments, children) {
        this.segments = segments;
        this.children = children;
        /**
         * The parent node in the url tree
         */
        this.parent = null;
        forEach(children, (/**
         * @template THIS
         * @this {THIS}
         * @param {?} v
         * @param {?} k
         * @return {THIS}
         */
        (v, k) => v.parent = this));
    }
    /**
     * Whether the segment has child segments
     * @return {?}
     */
    hasChildren() { return this.numberOfChildren > 0; }
    /**
     * Number of child segments
     * @return {?}
     */
    get numberOfChildren() { return Object.keys(this.children).length; }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return serializePaths(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlSegmentGroup.prototype._sourceSegment;
    /**
     * \@internal
     * @type {?}
     */
    UrlSegmentGroup.prototype._segmentIndexShift;
    /**
     * The parent node in the url tree
     * @type {?}
     */
    UrlSegmentGroup.prototype.parent;
    /**
     * The URL segments of this group. See `UrlSegment` for more information
     * @type {?}
     */
    UrlSegmentGroup.prototype.segments;
    /**
     * The list of children of this group
     * @type {?}
     */
    UrlSegmentGroup.prototype.children;
}
/**
 * \@description
 *
 * Represents a single URL segment.
 *
 * A UrlSegment is a part of a URL between the two slashes. It contains a path and the matrix
 * parameters associated with the segment.
 *
 * \@usageNotes
 *  ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const tree: UrlTree = router.parseUrl('/team;id=33');
 *     const g: UrlSegmentGroup = tree.root.children[PRIMARY_OUTLET];
 *     const s: UrlSegment[] = g.segments;
 *     s[0].path; // returns 'team'
 *     s[0].parameters; // returns {id: 33}
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
class UrlSegment {
    /**
     * @param {?} path
     * @param {?} parameters
     */
    constructor(path, parameters) {
        this.path = path;
        this.parameters = parameters;
    }
    /**
     * @return {?}
     */
    get parameterMap() {
        if (!this._parameterMap) {
            this._parameterMap = convertToParamMap(this.parameters);
        }
        return this._parameterMap;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return serializePath(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlSegment.prototype._parameterMap;
    /**
     * The path part of a URL segment
     * @type {?}
     */
    UrlSegment.prototype.path;
    /**
     * The matrix parameters associated with a segment
     * @type {?}
     */
    UrlSegment.prototype.parameters;
}
/**
 * @param {?} as
 * @param {?} bs
 * @return {?}
 */
function equalSegments(as, bs) {
    return equalPath(as, bs) && as.every((/**
     * @param {?} a
     * @param {?} i
     * @return {?}
     */
    (a, i) => shallowEqual(a.parameters, bs[i].parameters)));
}
/**
 * @param {?} as
 * @param {?} bs
 * @return {?}
 */
function equalPath(as, bs) {
    if (as.length !== bs.length)
        return false;
    return as.every((/**
     * @param {?} a
     * @param {?} i
     * @return {?}
     */
    (a, i) => a.path === bs[i].path));
}
/**
 * @template T
 * @param {?} segment
 * @param {?} fn
 * @return {?}
 */
function mapChildrenIntoArray(segment, fn) {
    /** @type {?} */
    let res = [];
    forEach(segment.children, (/**
     * @param {?} child
     * @param {?} childOutlet
     * @return {?}
     */
    (child, childOutlet) => {
        if (childOutlet === PRIMARY_OUTLET) {
            res = res.concat(fn(child, childOutlet));
        }
    }));
    forEach(segment.children, (/**
     * @param {?} child
     * @param {?} childOutlet
     * @return {?}
     */
    (child, childOutlet) => {
        if (childOutlet !== PRIMARY_OUTLET) {
            res = res.concat(fn(child, childOutlet));
        }
    }));
    return res;
}
/**
 * \@description
 *
 * Serializes and deserializes a URL string into a URL tree.
 *
 * The url serialization strategy is customizable. You can
 * make all URLs case insensitive by providing a custom UrlSerializer.
 *
 * See `DefaultUrlSerializer` for an example of a URL serializer.
 *
 * \@publicApi
 * @abstract
 */
class UrlSerializer {
}
if (false) {
    /**
     * Parse a url into a `UrlTree`
     * @abstract
     * @param {?} url
     * @return {?}
     */
    UrlSerializer.prototype.parse = function (url) { };
    /**
     * Converts a `UrlTree` into a url
     * @abstract
     * @param {?} tree
     * @return {?}
     */
    UrlSerializer.prototype.serialize = function (tree) { };
}
/**
 * \@description
 *
 * A default implementation of the `UrlSerializer`.
 *
 * Example URLs:
 *
 * ```
 * /inbox/33(popup:compose)
 * /inbox/33;open=true/messages/44
 * ```
 *
 * DefaultUrlSerializer uses parentheses to serialize secondary segments (e.g., popup:compose), the
 * colon syntax to specify the outlet, and the ';parameter=value' syntax (e.g., open=true) to
 * specify route specific parameters.
 *
 * \@publicApi
 */
class DefaultUrlSerializer {
    /**
     * Parses a url into a `UrlTree`
     * @param {?} url
     * @return {?}
     */
    parse(url) {
        /** @type {?} */
        const p = new UrlParser(url);
        return new UrlTree(p.parseRootSegment(), p.parseQueryParams(), p.parseFragment());
    }
    /**
     * Converts a `UrlTree` into a url
     * @param {?} tree
     * @return {?}
     */
    serialize(tree) {
        /** @type {?} */
        const segment = `/${serializeSegment(tree.root, true)}`;
        /** @type {?} */
        const query = serializeQueryParams(tree.queryParams);
        /** @type {?} */
        const fragment = typeof tree.fragment === `string` ? `#${encodeUriFragment((/** @type {?} */ (tree.fragment)))}` : '';
        return `${segment}${query}${fragment}`;
    }
}
/** @type {?} */
const DEFAULT_SERIALIZER = new DefaultUrlSerializer();
/**
 * @param {?} segment
 * @return {?}
 */
function serializePaths(segment) {
    return segment.segments.map((/**
     * @param {?} p
     * @return {?}
     */
    p => serializePath(p))).join('/');
}
/**
 * @param {?} segment
 * @param {?} root
 * @return {?}
 */
function serializeSegment(segment, root) {
    if (!segment.hasChildren()) {
        return serializePaths(segment);
    }
    if (root) {
        /** @type {?} */
        const primary = segment.children[PRIMARY_OUTLET] ?
            serializeSegment(segment.children[PRIMARY_OUTLET], false) :
            '';
        /** @type {?} */
        const children = [];
        forEach(segment.children, (/**
         * @param {?} v
         * @param {?} k
         * @return {?}
         */
        (v, k) => {
            if (k !== PRIMARY_OUTLET) {
                children.push(`${k}:${serializeSegment(v, false)}`);
            }
        }));
        return children.length > 0 ? `${primary}(${children.join('//')})` : primary;
    }
    else {
        /** @type {?} */
        const children = mapChildrenIntoArray(segment, (/**
         * @param {?} v
         * @param {?} k
         * @return {?}
         */
        (v, k) => {
            if (k === PRIMARY_OUTLET) {
                return [serializeSegment(segment.children[PRIMARY_OUTLET], false)];
            }
            return [`${k}:${serializeSegment(v, false)}`];
        }));
        return `${serializePaths(segment)}/(${children.join('//')})`;
    }
}
/**
 * Encodes a URI string with the default encoding. This function will only ever be called from
 * `encodeUriQuery` or `encodeUriSegment` as it's the base set of encodings to be used. We need
 * a custom encoding because encodeURIComponent is too aggressive and encodes stuff that doesn't
 * have to be encoded per https://url.spec.whatwg.org.
 * @param {?} s
 * @return {?}
 */
function encodeUriString(s) {
    return encodeURIComponent(s)
        .replace(/%40/g, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/g, '$')
        .replace(/%2C/gi, ',');
}
/**
 * This function should be used to encode both keys and values in a query string key/value. In
 * the following URL, you need to call encodeUriQuery on "k" and "v":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
function encodeUriQuery(s) {
    return encodeUriString(s).replace(/%3B/gi, ';');
}
/**
 * This function should be used to encode a URL fragment. In the following URL, you need to call
 * encodeUriFragment on "f":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
function encodeUriFragment(s) {
    return encodeURI(s);
}
/**
 * This function should be run on any URI segment as well as the key and value in a key/value
 * pair for matrix params. In the following URL, you need to call encodeUriSegment on "html",
 * "mk", and "mv":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
function encodeUriSegment(s) {
    return encodeUriString(s).replace(/\(/g, '%28').replace(/\)/g, '%29').replace(/%26/gi, '&');
}
/**
 * @param {?} s
 * @return {?}
 */
function decode(s) {
    return decodeURIComponent(s);
}
// Query keys/values should have the "+" replaced first, as "+" in a query string is " ".
// decodeURIComponent function will not decode "+" as a space.
/**
 * @param {?} s
 * @return {?}
 */
function decodeQuery(s) {
    return decode(s.replace(/\+/g, '%20'));
}
/**
 * @param {?} path
 * @return {?}
 */
function serializePath(path) {
    return `${encodeUriSegment(path.path)}${serializeMatrixParams(path.parameters)}`;
}
/**
 * @param {?} params
 * @return {?}
 */
function serializeMatrixParams(params) {
    return Object.keys(params)
        .map((/**
     * @param {?} key
     * @return {?}
     */
    key => `;${encodeUriSegment(key)}=${encodeUriSegment(params[key])}`))
        .join('');
}
/**
 * @param {?} params
 * @return {?}
 */
function serializeQueryParams(params) {
    /** @type {?} */
    const strParams = Object.keys(params).map((/**
     * @param {?} name
     * @return {?}
     */
    (name) => {
        /** @type {?} */
        const value = params[name];
        return Array.isArray(value) ?
            value.map((/**
             * @param {?} v
             * @return {?}
             */
            v => `${encodeUriQuery(name)}=${encodeUriQuery(v)}`)).join('&') :
            `${encodeUriQuery(name)}=${encodeUriQuery(value)}`;
    }));
    return strParams.length ? `?${strParams.join("&")}` : '';
}
/** @type {?} */
const SEGMENT_RE = /^[^\/()?;=#]+/;
/**
 * @param {?} str
 * @return {?}
 */
function matchSegments(str) {
    /** @type {?} */
    const match = str.match(SEGMENT_RE);
    return match ? match[0] : '';
}
/** @type {?} */
const QUERY_PARAM_RE = /^[^=?&#]+/;
// Return the name of the query param at the start of the string or an empty string
/**
 * @param {?} str
 * @return {?}
 */
function matchQueryParams(str) {
    /** @type {?} */
    const match = str.match(QUERY_PARAM_RE);
    return match ? match[0] : '';
}
/** @type {?} */
const QUERY_PARAM_VALUE_RE = /^[^?&#]+/;
// Return the value of the query param at the start of the string or an empty string
/**
 * @param {?} str
 * @return {?}
 */
function matchUrlQueryParamValue(str) {
    /** @type {?} */
    const match = str.match(QUERY_PARAM_VALUE_RE);
    return match ? match[0] : '';
}
class UrlParser {
    /**
     * @param {?} url
     */
    constructor(url) {
        this.url = url;
        this.remaining = url;
    }
    /**
     * @return {?}
     */
    parseRootSegment() {
        this.consumeOptional('/');
        if (this.remaining === '' || this.peekStartsWith('?') || this.peekStartsWith('#')) {
            return new UrlSegmentGroup([], {});
        }
        // The root segment group never has segments
        return new UrlSegmentGroup([], this.parseChildren());
    }
    /**
     * @return {?}
     */
    parseQueryParams() {
        /** @type {?} */
        const params = {};
        if (this.consumeOptional('?')) {
            do {
                this.parseQueryParam(params);
            } while (this.consumeOptional('&'));
        }
        return params;
    }
    /**
     * @return {?}
     */
    parseFragment() {
        return this.consumeOptional('#') ? decodeURIComponent(this.remaining) : null;
    }
    /**
     * @private
     * @return {?}
     */
    parseChildren() {
        if (this.remaining === '') {
            return {};
        }
        this.consumeOptional('/');
        /** @type {?} */
        const segments = [];
        if (!this.peekStartsWith('(')) {
            segments.push(this.parseSegment());
        }
        while (this.peekStartsWith('/') && !this.peekStartsWith('//') && !this.peekStartsWith('/(')) {
            this.capture('/');
            segments.push(this.parseSegment());
        }
        /** @type {?} */
        let children = {};
        if (this.peekStartsWith('/(')) {
            this.capture('/');
            children = this.parseParens(true);
        }
        /** @type {?} */
        let res = {};
        if (this.peekStartsWith('(')) {
            res = this.parseParens(false);
        }
        if (segments.length > 0 || Object.keys(children).length > 0) {
            res[PRIMARY_OUTLET] = new UrlSegmentGroup(segments, children);
        }
        return res;
    }
    // parse a segment with its matrix parameters
    // ie `name;k1=v1;k2`
    /**
     * @private
     * @return {?}
     */
    parseSegment() {
        /** @type {?} */
        const path = matchSegments(this.remaining);
        if (path === '' && this.peekStartsWith(';')) {
            throw new Error(`Empty path url segment cannot have parameters: '${this.remaining}'.`);
        }
        this.capture(path);
        return new UrlSegment(decode(path), this.parseMatrixParams());
    }
    /**
     * @private
     * @return {?}
     */
    parseMatrixParams() {
        /** @type {?} */
        const params = {};
        while (this.consumeOptional(';')) {
            this.parseParam(params);
        }
        return params;
    }
    /**
     * @private
     * @param {?} params
     * @return {?}
     */
    parseParam(params) {
        /** @type {?} */
        const key = matchSegments(this.remaining);
        if (!key) {
            return;
        }
        this.capture(key);
        /** @type {?} */
        let value = '';
        if (this.consumeOptional('=')) {
            /** @type {?} */
            const valueMatch = matchSegments(this.remaining);
            if (valueMatch) {
                value = valueMatch;
                this.capture(value);
            }
        }
        params[decode(key)] = decode(value);
    }
    // Parse a single query parameter `name[=value]`
    /**
     * @private
     * @param {?} params
     * @return {?}
     */
    parseQueryParam(params) {
        /** @type {?} */
        const key = matchQueryParams(this.remaining);
        if (!key) {
            return;
        }
        this.capture(key);
        /** @type {?} */
        let value = '';
        if (this.consumeOptional('=')) {
            /** @type {?} */
            const valueMatch = matchUrlQueryParamValue(this.remaining);
            if (valueMatch) {
                value = valueMatch;
                this.capture(value);
            }
        }
        /** @type {?} */
        const decodedKey = decodeQuery(key);
        /** @type {?} */
        const decodedVal = decodeQuery(value);
        if (params.hasOwnProperty(decodedKey)) {
            // Append to existing values
            /** @type {?} */
            let currentVal = params[decodedKey];
            if (!Array.isArray(currentVal)) {
                currentVal = [currentVal];
                params[decodedKey] = currentVal;
            }
            currentVal.push(decodedVal);
        }
        else {
            // Create a new value
            params[decodedKey] = decodedVal;
        }
    }
    // parse `(a/b//outlet_name:c/d)`
    /**
     * @private
     * @param {?} allowPrimary
     * @return {?}
     */
    parseParens(allowPrimary) {
        /** @type {?} */
        const segments = {};
        this.capture('(');
        while (!this.consumeOptional(')') && this.remaining.length > 0) {
            /** @type {?} */
            const path = matchSegments(this.remaining);
            /** @type {?} */
            const next = this.remaining[path.length];
            // if is is not one of these characters, then the segment was unescaped
            // or the group was not closed
            if (next !== '/' && next !== ')' && next !== ';') {
                throw new Error(`Cannot parse url '${this.url}'`);
            }
            /** @type {?} */
            let outletName = (/** @type {?} */ (undefined));
            if (path.indexOf(':') > -1) {
                outletName = path.substr(0, path.indexOf(':'));
                this.capture(outletName);
                this.capture(':');
            }
            else if (allowPrimary) {
                outletName = PRIMARY_OUTLET;
            }
            /** @type {?} */
            const children = this.parseChildren();
            segments[outletName] = Object.keys(children).length === 1 ? children[PRIMARY_OUTLET] :
                new UrlSegmentGroup([], children);
            this.consumeOptional('//');
        }
        return segments;
    }
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    peekStartsWith(str) { return this.remaining.startsWith(str); }
    // Consumes the prefix when it is present and returns whether it has been consumed
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    consumeOptional(str) {
        if (this.peekStartsWith(str)) {
            this.remaining = this.remaining.substring(str.length);
            return true;
        }
        return false;
    }
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    capture(str) {
        if (!this.consumeOptional(str)) {
            throw new Error(`Expected "${str}".`);
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    UrlParser.prototype.remaining;
    /**
     * @type {?}
     * @private
     */
    UrlParser.prototype.url;
}

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
/**
 * @template T
 */
class Tree {
    /**
     * @param {?} root
     */
    constructor(root) { this._root = root; }
    /**
     * @return {?}
     */
    get root() { return this._root.value; }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    parent(t) {
        /** @type {?} */
        const p = this.pathFromRoot(t);
        return p.length > 1 ? p[p.length - 2] : null;
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    children(t) {
        /** @type {?} */
        const n = findNode(t, this._root);
        return n ? n.children.map((/**
         * @param {?} t
         * @return {?}
         */
        t => t.value)) : [];
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    firstChild(t) {
        /** @type {?} */
        const n = findNode(t, this._root);
        return n && n.children.length > 0 ? n.children[0].value : null;
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    siblings(t) {
        /** @type {?} */
        const p = findPath(t, this._root);
        if (p.length < 2)
            return [];
        /** @type {?} */
        const c = p[p.length - 2].children.map((/**
         * @param {?} c
         * @return {?}
         */
        c => c.value));
        return c.filter((/**
         * @param {?} cc
         * @return {?}
         */
        cc => cc !== t));
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    pathFromRoot(t) { return findPath(t, this._root).map((/**
     * @param {?} s
     * @return {?}
     */
    s => s.value)); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    Tree.prototype._root;
}
// DFS for the node matching the value
/**
 * @template T
 * @param {?} value
 * @param {?} node
 * @return {?}
 */
function findNode(value, node) {
    if (value === node.value)
        return node;
    for (const child of node.children) {
        /** @type {?} */
        const node = findNode(value, child);
        if (node)
            return node;
    }
    return null;
}
// Return the path to the node with the given value using DFS
/**
 * @template T
 * @param {?} value
 * @param {?} node
 * @return {?}
 */
function findPath(value, node) {
    if (value === node.value)
        return [node];
    for (const child of node.children) {
        /** @type {?} */
        const path = findPath(value, child);
        if (path.length) {
            path.unshift(node);
            return path;
        }
    }
    return [];
}
/**
 * @template T
 */
class TreeNode {
    /**
     * @param {?} value
     * @param {?} children
     */
    constructor(value, children) {
        this.value = value;
        this.children = children;
    }
    /**
     * @return {?}
     */
    toString() { return `TreeNode(${this.value})`; }
}
if (false) {
    /** @type {?} */
    TreeNode.prototype.value;
    /** @type {?} */
    TreeNode.prototype.children;
}
// Return the list of T indexed by outlet name
/**
 * @template T
 * @param {?} node
 * @return {?}
 */
function nodeChildrenAsMap(node) {
    /** @type {?} */
    const map = {};
    if (node) {
        node.children.forEach((/**
         * @param {?} child
         * @return {?}
         */
        child => map[child.value.outlet] = child));
    }
    return map;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Represents the state of the router as a tree of activated routes.
 *
 * \@usageNotes
 *
 * Every node in the route tree is an `ActivatedRoute` instance
 * that knows about the "consumed" URL segments, the extracted parameters,
 * and the resolved data.
 * Use the `ActivatedRoute` properties to traverse the tree from any node.
 *
 * ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const state: RouterState = router.routerState;
 *     const root: ActivatedRoute = state.root;
 *     const child = root.firstChild;
 *     const id: Observable<string> = child.params.map(p => p.id);
 *     //...
 *   }
 * }
 * ```
 *
 * @see `ActivatedRoute`
 *
 * \@publicApi
 */
class RouterState extends Tree {
    /**
     * \@internal
     * @param {?} root
     * @param {?} snapshot
     */
    constructor(root, snapshot) {
        super(root);
        this.snapshot = snapshot;
        setRouterState((/** @type {?} */ (this)), root);
    }
    /**
     * @return {?}
     */
    toString() { return this.snapshot.toString(); }
}
if (false) {
    /**
     * The current snapshot of the router state
     * @type {?}
     */
    RouterState.prototype.snapshot;
}
/**
 * @param {?} urlTree
 * @param {?} rootComponent
 * @return {?}
 */
function createEmptyState(urlTree, rootComponent) {
    /** @type {?} */
    const snapshot = createEmptyStateSnapshot(urlTree, rootComponent);
    /** @type {?} */
    const emptyUrl = new BehaviorSubject([new UrlSegment('', {})]);
    /** @type {?} */
    const emptyParams = new BehaviorSubject({});
    /** @type {?} */
    const emptyData = new BehaviorSubject({});
    /** @type {?} */
    const emptyQueryParams = new BehaviorSubject({});
    /** @type {?} */
    const fragment = new BehaviorSubject('');
    /** @type {?} */
    const activated = new ActivatedRoute(emptyUrl, emptyParams, emptyQueryParams, fragment, emptyData, PRIMARY_OUTLET, rootComponent, snapshot.root);
    activated.snapshot = snapshot.root;
    return new RouterState(new TreeNode(activated, []), snapshot);
}
/**
 * @param {?} urlTree
 * @param {?} rootComponent
 * @return {?}
 */
function createEmptyStateSnapshot(urlTree, rootComponent) {
    /** @type {?} */
    const emptyParams = {};
    /** @type {?} */
    const emptyData = {};
    /** @type {?} */
    const emptyQueryParams = {};
    /** @type {?} */
    const fragment = '';
    /** @type {?} */
    const activated = new ActivatedRouteSnapshot([], emptyParams, emptyQueryParams, fragment, emptyData, PRIMARY_OUTLET, rootComponent, null, urlTree.root, -1, {});
    return new RouterStateSnapshot('', new TreeNode(activated, []));
}
/**
 * Provides access to information about a route associated with a component
 * that is loaded in an outlet.
 * Use to traverse the `RouterState` tree and extract information from nodes.
 *
 * {\@example router/activated-route/module.ts region="activated-route"
 *     header="activated-route.component.ts"}
 *
 * \@publicApi
 */
class ActivatedRoute {
    /**
     * \@internal
     * @param {?} url
     * @param {?} params
     * @param {?} queryParams
     * @param {?} fragment
     * @param {?} data
     * @param {?} outlet
     * @param {?} component
     * @param {?} futureSnapshot
     */
    constructor(url, params, queryParams, fragment, data, outlet, component, futureSnapshot) {
        this.url = url;
        this.params = params;
        this.queryParams = queryParams;
        this.fragment = fragment;
        this.data = data;
        this.outlet = outlet;
        this.component = component;
        this._futureSnapshot = futureSnapshot;
    }
    /**
     * The configuration used to match this route.
     * @return {?}
     */
    get routeConfig() { return this._futureSnapshot.routeConfig; }
    /**
     * The root of the router state.
     * @return {?}
     */
    get root() { return this._routerState.root; }
    /**
     * The parent of this route in the router state tree.
     * @return {?}
     */
    get parent() { return this._routerState.parent(this); }
    /**
     * The first child of this route in the router state tree.
     * @return {?}
     */
    get firstChild() { return this._routerState.firstChild(this); }
    /**
     * The children of this route in the router state tree.
     * @return {?}
     */
    get children() { return this._routerState.children(this); }
    /**
     * The path from the root of the router state tree to this route.
     * @return {?}
     */
    get pathFromRoot() { return this._routerState.pathFromRoot(this); }
    /**
     * An Observable that contains a map of the required and optional parameters
     * specific to the route.
     * The map supports retrieving single and multiple values from the same parameter.
     * @return {?}
     */
    get paramMap() {
        if (!this._paramMap) {
            this._paramMap = this.params.pipe(map((/**
             * @param {?} p
             * @return {?}
             */
            (p) => convertToParamMap(p))));
        }
        return this._paramMap;
    }
    /**
     * An Observable that contains a map of the query parameters available to all routes.
     * The map supports retrieving single and multiple values from the query parameter.
     * @return {?}
     */
    get queryParamMap() {
        if (!this._queryParamMap) {
            this._queryParamMap =
                this.queryParams.pipe(map((/**
                 * @param {?} p
                 * @return {?}
                 */
                (p) => convertToParamMap(p))));
        }
        return this._queryParamMap;
    }
    /**
     * @return {?}
     */
    toString() {
        return this.snapshot ? this.snapshot.toString() : `Future(${this._futureSnapshot})`;
    }
}
if (false) {
    /**
     * The current snapshot of this route
     * @type {?}
     */
    ActivatedRoute.prototype.snapshot;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRoute.prototype._futureSnapshot;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRoute.prototype._routerState;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRoute.prototype._paramMap;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRoute.prototype._queryParamMap;
    /**
     * An observable of the URL segments matched by this route.
     * @type {?}
     */
    ActivatedRoute.prototype.url;
    /**
     * An observable of the matrix parameters scoped to this route.
     * @type {?}
     */
    ActivatedRoute.prototype.params;
    /**
     * An observable of the query parameters shared by all the routes.
     * @type {?}
     */
    ActivatedRoute.prototype.queryParams;
    /**
     * An observable of the URL fragment shared by all the routes.
     * @type {?}
     */
    ActivatedRoute.prototype.fragment;
    /**
     * An observable of the static and resolved data of this route.
     * @type {?}
     */
    ActivatedRoute.prototype.data;
    /**
     * The outlet name of the route, a constant.
     * @type {?}
     */
    ActivatedRoute.prototype.outlet;
    /**
     * The component of the route, a constant.
     * @type {?}
     */
    ActivatedRoute.prototype.component;
}
/**
 * Returns the inherited params, data, and resolve for a given route.
 * By default, this only inherits values up to the nearest path-less or component-less route.
 * \@internal
 * @param {?} route
 * @param {?=} paramsInheritanceStrategy
 * @return {?}
 */
function inheritedParamsDataResolve(route, paramsInheritanceStrategy = 'emptyOnly') {
    /** @type {?} */
    const pathFromRoot = route.pathFromRoot;
    /** @type {?} */
    let inheritingStartingFrom = 0;
    if (paramsInheritanceStrategy !== 'always') {
        inheritingStartingFrom = pathFromRoot.length - 1;
        while (inheritingStartingFrom >= 1) {
            /** @type {?} */
            const current = pathFromRoot[inheritingStartingFrom];
            /** @type {?} */
            const parent = pathFromRoot[inheritingStartingFrom - 1];
            // current route is an empty path => inherits its parent's params and data
            if (current.routeConfig && current.routeConfig.path === '') {
                inheritingStartingFrom--;
                // parent is componentless => current route should inherit its params and data
            }
            else if (!parent.component) {
                inheritingStartingFrom--;
            }
            else {
                break;
            }
        }
    }
    return flattenInherited(pathFromRoot.slice(inheritingStartingFrom));
}
/**
 * \@internal
 * @param {?} pathFromRoot
 * @return {?}
 */
function flattenInherited(pathFromRoot) {
    return pathFromRoot.reduce((/**
     * @param {?} res
     * @param {?} curr
     * @return {?}
     */
    (res, curr) => {
        /** @type {?} */
        const params = Object.assign({}, res.params, curr.params);
        /** @type {?} */
        const data = Object.assign({}, res.data, curr.data);
        /** @type {?} */
        const resolve = Object.assign({}, res.resolve, curr._resolvedData);
        return { params, data, resolve };
    }), (/** @type {?} */ ({ params: {}, data: {}, resolve: {} })));
}
/**
 * \@description
 *
 * Contains the information about a route associated with a component loaded in an
 * outlet at a particular moment in time. ActivatedRouteSnapshot can also be used to
 * traverse the router state tree.
 *
 * ```
 * \@Component({templateUrl:'./my-component.html'})
 * class MyComponent {
 *   constructor(route: ActivatedRoute) {
 *     const id: string = route.snapshot.params.id;
 *     const url: string = route.snapshot.url.join('');
 *     const user = route.snapshot.data.user;
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
class ActivatedRouteSnapshot {
    /**
     * \@internal
     * @param {?} url
     * @param {?} params
     * @param {?} queryParams
     * @param {?} fragment
     * @param {?} data
     * @param {?} outlet
     * @param {?} component
     * @param {?} routeConfig
     * @param {?} urlSegment
     * @param {?} lastPathIndex
     * @param {?} resolve
     */
    constructor(url, params, queryParams, fragment, data, outlet, component, routeConfig, urlSegment, lastPathIndex, resolve) {
        this.url = url;
        this.params = params;
        this.queryParams = queryParams;
        this.fragment = fragment;
        this.data = data;
        this.outlet = outlet;
        this.component = component;
        this.routeConfig = routeConfig;
        this._urlSegment = urlSegment;
        this._lastPathIndex = lastPathIndex;
        this._resolve = resolve;
    }
    /**
     * The root of the router state
     * @return {?}
     */
    get root() { return this._routerState.root; }
    /**
     * The parent of this route in the router state tree
     * @return {?}
     */
    get parent() { return this._routerState.parent(this); }
    /**
     * The first child of this route in the router state tree
     * @return {?}
     */
    get firstChild() { return this._routerState.firstChild(this); }
    /**
     * The children of this route in the router state tree
     * @return {?}
     */
    get children() { return this._routerState.children(this); }
    /**
     * The path from the root of the router state tree to this route
     * @return {?}
     */
    get pathFromRoot() { return this._routerState.pathFromRoot(this); }
    /**
     * @return {?}
     */
    get paramMap() {
        if (!this._paramMap) {
            this._paramMap = convertToParamMap(this.params);
        }
        return this._paramMap;
    }
    /**
     * @return {?}
     */
    get queryParamMap() {
        if (!this._queryParamMap) {
            this._queryParamMap = convertToParamMap(this.queryParams);
        }
        return this._queryParamMap;
    }
    /**
     * @return {?}
     */
    toString() {
        /** @type {?} */
        const url = this.url.map((/**
         * @param {?} segment
         * @return {?}
         */
        segment => segment.toString())).join('/');
        /** @type {?} */
        const matched = this.routeConfig ? this.routeConfig.path : '';
        return `Route(url:'${url}', path:'${matched}')`;
    }
}
if (false) {
    /**
     * The configuration used to match this route *
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.routeConfig;
    /**
     * \@internal *
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._urlSegment;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._lastPathIndex;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._resolve;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._resolvedData;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._routerState;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._paramMap;
    /**
     * \@internal
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype._queryParamMap;
    /**
     * The URL segments matched by this route
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.url;
    /**
     * The matrix parameters scoped to this route
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.params;
    /**
     * The query parameters shared by all the routes
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.queryParams;
    /**
     * The URL fragment shared by all the routes
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.fragment;
    /**
     * The static and resolved data of this route
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.data;
    /**
     * The outlet name of the route
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.outlet;
    /**
     * The component of the route
     * @type {?}
     */
    ActivatedRouteSnapshot.prototype.component;
}
/**
 * \@description
 *
 * Represents the state of the router at a moment in time.
 *
 * This is a tree of activated route snapshots. Every node in this tree knows about
 * the "consumed" URL segments, the extracted parameters, and the resolved data.
 *
 * \@usageNotes
 * ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const state: RouterState = router.routerState;
 *     const snapshot: RouterStateSnapshot = state.snapshot;
 *     const root: ActivatedRouteSnapshot = snapshot.root;
 *     const child = root.firstChild;
 *     const id: Observable<string> = child.params.map(p => p.id);
 *     //...
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
class RouterStateSnapshot extends Tree {
    /**
     * \@internal
     * @param {?} url
     * @param {?} root
     */
    constructor(url, root) {
        super(root);
        this.url = url;
        setRouterState((/** @type {?} */ (this)), root);
    }
    /**
     * @return {?}
     */
    toString() { return serializeNode(this._root); }
}
if (false) {
    /**
     * The url from which this snapshot was created
     * @type {?}
     */
    RouterStateSnapshot.prototype.url;
}
/**
 * @template U, T
 * @param {?} state
 * @param {?} node
 * @return {?}
 */
function setRouterState(state, node) {
    node.value._routerState = state;
    node.children.forEach((/**
     * @param {?} c
     * @return {?}
     */
    c => setRouterState(state, c)));
}
/**
 * @param {?} node
 * @return {?}
 */
function serializeNode(node) {
    /** @type {?} */
    const c = node.children.length > 0 ? ` { ${node.children.map(serializeNode).join(', ')} } ` : '';
    return `${node.value}${c}`;
}
/**
 * The expectation is that the activate route is created with the right set of parameters.
 * So we push new values into the observables only when they are not the initial values.
 * And we detect that by checking if the snapshot field is set.
 * @param {?} route
 * @return {?}
 */
function advanceActivatedRoute(route) {
    if (route.snapshot) {
        /** @type {?} */
        const currentSnapshot = route.snapshot;
        /** @type {?} */
        const nextSnapshot = route._futureSnapshot;
        route.snapshot = nextSnapshot;
        if (!shallowEqual(currentSnapshot.queryParams, nextSnapshot.queryParams)) {
            ((/** @type {?} */ (route.queryParams))).next(nextSnapshot.queryParams);
        }
        if (currentSnapshot.fragment !== nextSnapshot.fragment) {
            ((/** @type {?} */ (route.fragment))).next(nextSnapshot.fragment);
        }
        if (!shallowEqual(currentSnapshot.params, nextSnapshot.params)) {
            ((/** @type {?} */ (route.params))).next(nextSnapshot.params);
        }
        if (!shallowEqualArrays(currentSnapshot.url, nextSnapshot.url)) {
            ((/** @type {?} */ (route.url))).next(nextSnapshot.url);
        }
        if (!shallowEqual(currentSnapshot.data, nextSnapshot.data)) {
            ((/** @type {?} */ (route.data))).next(nextSnapshot.data);
        }
    }
    else {
        route.snapshot = route._futureSnapshot;
        // this is for resolved data
        ((/** @type {?} */ (route.data))).next(route._futureSnapshot.data);
    }
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
function equalParamsAndUrlSegments(a, b) {
    /** @type {?} */
    const equalUrlParams = shallowEqual(a.params, b.params) && equalSegments(a.url, b.url);
    /** @type {?} */
    const parentsMismatch = !a.parent !== !b.parent;
    return equalUrlParams && !parentsMismatch &&
        (!a.parent || equalParamsAndUrlSegments(a.parent, (/** @type {?} */ (b.parent))));
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} routeReuseStrategy
 * @param {?} curr
 * @param {?} prevState
 * @return {?}
 */
function createRouterState(routeReuseStrategy, curr, prevState) {
    /** @type {?} */
    const root = createNode(routeReuseStrategy, curr._root, prevState ? prevState._root : undefined);
    return new RouterState(root, curr);
}
/**
 * @param {?} routeReuseStrategy
 * @param {?} curr
 * @param {?=} prevState
 * @return {?}
 */
function createNode(routeReuseStrategy, curr, prevState) {
    // reuse an activated route that is currently displayed on the screen
    if (prevState && routeReuseStrategy.shouldReuseRoute(curr.value, prevState.value.snapshot)) {
        /** @type {?} */
        const value = prevState.value;
        value._futureSnapshot = curr.value;
        /** @type {?} */
        const children = createOrReuseChildren(routeReuseStrategy, curr, prevState);
        return new TreeNode(value, children);
        // retrieve an activated route that is used to be displayed, but is not currently displayed
    }
    else {
        /** @type {?} */
        const detachedRouteHandle = (/** @type {?} */ (routeReuseStrategy.retrieve(curr.value)));
        if (detachedRouteHandle) {
            /** @type {?} */
            const tree = detachedRouteHandle.route;
            setFutureSnapshotsOfActivatedRoutes(curr, tree);
            return tree;
        }
        else {
            /** @type {?} */
            const value = createActivatedRoute(curr.value);
            /** @type {?} */
            const children = curr.children.map((/**
             * @param {?} c
             * @return {?}
             */
            c => createNode(routeReuseStrategy, c)));
            return new TreeNode(value, children);
        }
    }
}
/**
 * @param {?} curr
 * @param {?} result
 * @return {?}
 */
function setFutureSnapshotsOfActivatedRoutes(curr, result) {
    if (curr.value.routeConfig !== result.value.routeConfig) {
        throw new Error('Cannot reattach ActivatedRouteSnapshot created from a different route');
    }
    if (curr.children.length !== result.children.length) {
        throw new Error('Cannot reattach ActivatedRouteSnapshot with a different number of children');
    }
    result.value._futureSnapshot = curr.value;
    for (let i = 0; i < curr.children.length; ++i) {
        setFutureSnapshotsOfActivatedRoutes(curr.children[i], result.children[i]);
    }
}
/**
 * @param {?} routeReuseStrategy
 * @param {?} curr
 * @param {?} prevState
 * @return {?}
 */
function createOrReuseChildren(routeReuseStrategy, curr, prevState) {
    return curr.children.map((/**
     * @param {?} child
     * @return {?}
     */
    child => {
        for (const p of prevState.children) {
            if (routeReuseStrategy.shouldReuseRoute(p.value.snapshot, child.value)) {
                return createNode(routeReuseStrategy, child, p);
            }
        }
        return createNode(routeReuseStrategy, child);
    }));
}
/**
 * @param {?} c
 * @return {?}
 */
function createActivatedRoute(c) {
    return new ActivatedRoute(new BehaviorSubject(c.url), new BehaviorSubject(c.params), new BehaviorSubject(c.queryParams), new BehaviorSubject(c.fragment), new BehaviorSubject(c.data), c.outlet, c.component, c);
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} route
 * @param {?} urlTree
 * @param {?} commands
 * @param {?} queryParams
 * @param {?} fragment
 * @return {?}
 */
function createUrlTree(route, urlTree, commands, queryParams, fragment) {
    if (commands.length === 0) {
        return tree(urlTree.root, urlTree.root, urlTree, queryParams, fragment);
    }
    /** @type {?} */
    const nav = computeNavigation(commands);
    if (nav.toRoot()) {
        return tree(urlTree.root, new UrlSegmentGroup([], {}), urlTree, queryParams, fragment);
    }
    /** @type {?} */
    const startingPosition = findStartingPosition(nav, urlTree, route);
    /** @type {?} */
    const segmentGroup = startingPosition.processChildren ?
        updateSegmentGroupChildren(startingPosition.segmentGroup, startingPosition.index, nav.commands) :
        updateSegmentGroup(startingPosition.segmentGroup, startingPosition.index, nav.commands);
    return tree(startingPosition.segmentGroup, segmentGroup, urlTree, queryParams, fragment);
}
/**
 * @param {?} command
 * @return {?}
 */
function isMatrixParams(command) {
    return typeof command === 'object' && command != null && !command.outlets && !command.segmentPath;
}
/**
 * @param {?} oldSegmentGroup
 * @param {?} newSegmentGroup
 * @param {?} urlTree
 * @param {?} queryParams
 * @param {?} fragment
 * @return {?}
 */
function tree(oldSegmentGroup, newSegmentGroup, urlTree, queryParams, fragment) {
    /** @type {?} */
    let qp = {};
    if (queryParams) {
        forEach(queryParams, (/**
         * @param {?} value
         * @param {?} name
         * @return {?}
         */
        (value, name) => {
            qp[name] = Array.isArray(value) ? value.map((/**
             * @param {?} v
             * @return {?}
             */
            (v) => `${v}`)) : `${value}`;
        }));
    }
    if (urlTree.root === oldSegmentGroup) {
        return new UrlTree(newSegmentGroup, qp, fragment);
    }
    return new UrlTree(replaceSegment(urlTree.root, oldSegmentGroup, newSegmentGroup), qp, fragment);
}
/**
 * @param {?} current
 * @param {?} oldSegment
 * @param {?} newSegment
 * @return {?}
 */
function replaceSegment(current, oldSegment, newSegment) {
    /** @type {?} */
    const children = {};
    forEach(current.children, (/**
     * @param {?} c
     * @param {?} outletName
     * @return {?}
     */
    (c, outletName) => {
        if (c === oldSegment) {
            children[outletName] = newSegment;
        }
        else {
            children[outletName] = replaceSegment(c, oldSegment, newSegment);
        }
    }));
    return new UrlSegmentGroup(current.segments, children);
}
class Navigation {
    /**
     * @param {?} isAbsolute
     * @param {?} numberOfDoubleDots
     * @param {?} commands
     */
    constructor(isAbsolute, numberOfDoubleDots, commands) {
        this.isAbsolute = isAbsolute;
        this.numberOfDoubleDots = numberOfDoubleDots;
        this.commands = commands;
        if (isAbsolute && commands.length > 0 && isMatrixParams(commands[0])) {
            throw new Error('Root segment cannot have matrix parameters');
        }
        /** @type {?} */
        const cmdWithOutlet = commands.find((/**
         * @param {?} c
         * @return {?}
         */
        c => typeof c === 'object' && c != null && c.outlets));
        if (cmdWithOutlet && cmdWithOutlet !== last(commands)) {
            throw new Error('{outlets:{}} has to be the last command');
        }
    }
    /**
     * @return {?}
     */
    toRoot() {
        return this.isAbsolute && this.commands.length === 1 && this.commands[0] == '/';
    }
}
if (false) {
    /** @type {?} */
    Navigation.prototype.isAbsolute;
    /** @type {?} */
    Navigation.prototype.numberOfDoubleDots;
    /** @type {?} */
    Navigation.prototype.commands;
}
/**
 * Transforms commands to a normalized `Navigation`
 * @param {?} commands
 * @return {?}
 */
function computeNavigation(commands) {
    if ((typeof commands[0] === 'string') && commands.length === 1 && commands[0] === '/') {
        return new Navigation(true, 0, commands);
    }
    /** @type {?} */
    let numberOfDoubleDots = 0;
    /** @type {?} */
    let isAbsolute = false;
    /** @type {?} */
    const res = commands.reduce((/**
     * @param {?} res
     * @param {?} cmd
     * @param {?} cmdIdx
     * @return {?}
     */
    (res, cmd, cmdIdx) => {
        if (typeof cmd === 'object' && cmd != null) {
            if (cmd.outlets) {
                /** @type {?} */
                const outlets = {};
                forEach(cmd.outlets, (/**
                 * @param {?} commands
                 * @param {?} name
                 * @return {?}
                 */
                (commands, name) => {
                    outlets[name] = typeof commands === 'string' ? commands.split('/') : commands;
                }));
                return [...res, { outlets }];
            }
            if (cmd.segmentPath) {
                return [...res, cmd.segmentPath];
            }
        }
        if (!(typeof cmd === 'string')) {
            return [...res, cmd];
        }
        if (cmdIdx === 0) {
            cmd.split('/').forEach((/**
             * @param {?} urlPart
             * @param {?} partIndex
             * @return {?}
             */
            (urlPart, partIndex) => {
                if (partIndex == 0 && urlPart === '.') {
                    // skip './a'
                }
                else if (partIndex == 0 && urlPart === '') { //  '/a'
                    isAbsolute = true;
                }
                else if (urlPart === '..') { //  '../a'
                    numberOfDoubleDots++;
                }
                else if (urlPart != '') {
                    res.push(urlPart);
                }
            }));
            return res;
        }
        return [...res, cmd];
    }), []);
    return new Navigation(isAbsolute, numberOfDoubleDots, res);
}
class Position {
    /**
     * @param {?} segmentGroup
     * @param {?} processChildren
     * @param {?} index
     */
    constructor(segmentGroup, processChildren, index) {
        this.segmentGroup = segmentGroup;
        this.processChildren = processChildren;
        this.index = index;
    }
}
if (false) {
    /** @type {?} */
    Position.prototype.segmentGroup;
    /** @type {?} */
    Position.prototype.processChildren;
    /** @type {?} */
    Position.prototype.index;
}
/**
 * @param {?} nav
 * @param {?} tree
 * @param {?} route
 * @return {?}
 */
function findStartingPosition(nav, tree, route) {
    if (nav.isAbsolute) {
        return new Position(tree.root, true, 0);
    }
    if (route.snapshot._lastPathIndex === -1) {
        return new Position(route.snapshot._urlSegment, true, 0);
    }
    /** @type {?} */
    const modifier = isMatrixParams(nav.commands[0]) ? 0 : 1;
    /** @type {?} */
    const index = route.snapshot._lastPathIndex + modifier;
    return createPositionApplyingDoubleDots(route.snapshot._urlSegment, index, nav.numberOfDoubleDots);
}
/**
 * @param {?} group
 * @param {?} index
 * @param {?} numberOfDoubleDots
 * @return {?}
 */
function createPositionApplyingDoubleDots(group, index, numberOfDoubleDots) {
    /** @type {?} */
    let g = group;
    /** @type {?} */
    let ci = index;
    /** @type {?} */
    let dd = numberOfDoubleDots;
    while (dd > ci) {
        dd -= ci;
        g = (/** @type {?} */ (g.parent));
        if (!g) {
            throw new Error('Invalid number of \'../\'');
        }
        ci = g.segments.length;
    }
    return new Position(g, false, ci - dd);
}
/**
 * @param {?} command
 * @return {?}
 */
function getPath(command) {
    if (typeof command === 'object' && command != null && command.outlets) {
        return command.outlets[PRIMARY_OUTLET];
    }
    return `${command}`;
}
/**
 * @param {?} commands
 * @return {?}
 */
function getOutlets(commands) {
    if (!(typeof commands[0] === 'object'))
        return { [PRIMARY_OUTLET]: commands };
    if (commands[0].outlets === undefined)
        return { [PRIMARY_OUTLET]: commands };
    return commands[0].outlets;
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function updateSegmentGroup(segmentGroup, startIndex, commands) {
    if (!segmentGroup) {
        segmentGroup = new UrlSegmentGroup([], {});
    }
    if (segmentGroup.segments.length === 0 && segmentGroup.hasChildren()) {
        return updateSegmentGroupChildren(segmentGroup, startIndex, commands);
    }
    /** @type {?} */
    const m = prefixedWith(segmentGroup, startIndex, commands);
    /** @type {?} */
    const slicedCommands = commands.slice(m.commandIndex);
    if (m.match && m.pathIndex < segmentGroup.segments.length) {
        /** @type {?} */
        const g = new UrlSegmentGroup(segmentGroup.segments.slice(0, m.pathIndex), {});
        g.children[PRIMARY_OUTLET] =
            new UrlSegmentGroup(segmentGroup.segments.slice(m.pathIndex), segmentGroup.children);
        return updateSegmentGroupChildren(g, 0, slicedCommands);
    }
    else if (m.match && slicedCommands.length === 0) {
        return new UrlSegmentGroup(segmentGroup.segments, {});
    }
    else if (m.match && !segmentGroup.hasChildren()) {
        return createNewSegmentGroup(segmentGroup, startIndex, commands);
    }
    else if (m.match) {
        return updateSegmentGroupChildren(segmentGroup, 0, slicedCommands);
    }
    else {
        return createNewSegmentGroup(segmentGroup, startIndex, commands);
    }
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function updateSegmentGroupChildren(segmentGroup, startIndex, commands) {
    if (commands.length === 0) {
        return new UrlSegmentGroup(segmentGroup.segments, {});
    }
    else {
        /** @type {?} */
        const outlets = getOutlets(commands);
        /** @type {?} */
        const children = {};
        forEach(outlets, (/**
         * @param {?} commands
         * @param {?} outlet
         * @return {?}
         */
        (commands, outlet) => {
            if (commands !== null) {
                children[outlet] = updateSegmentGroup(segmentGroup.children[outlet], startIndex, commands);
            }
        }));
        forEach(segmentGroup.children, (/**
         * @param {?} child
         * @param {?} childOutlet
         * @return {?}
         */
        (child, childOutlet) => {
            if (outlets[childOutlet] === undefined) {
                children[childOutlet] = child;
            }
        }));
        return new UrlSegmentGroup(segmentGroup.segments, children);
    }
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function prefixedWith(segmentGroup, startIndex, commands) {
    /** @type {?} */
    let currentCommandIndex = 0;
    /** @type {?} */
    let currentPathIndex = startIndex;
    /** @type {?} */
    const noMatch = { match: false, pathIndex: 0, commandIndex: 0 };
    while (currentPathIndex < segmentGroup.segments.length) {
        if (currentCommandIndex >= commands.length)
            return noMatch;
        /** @type {?} */
        const path = segmentGroup.segments[currentPathIndex];
        /** @type {?} */
        const curr = getPath(commands[currentCommandIndex]);
        /** @type {?} */
        const next = currentCommandIndex < commands.length - 1 ? commands[currentCommandIndex + 1] : null;
        if (currentPathIndex > 0 && curr === undefined)
            break;
        if (curr && next && (typeof next === 'object') && next.outlets === undefined) {
            if (!compare(curr, next, path))
                return noMatch;
            currentCommandIndex += 2;
        }
        else {
            if (!compare(curr, {}, path))
                return noMatch;
            currentCommandIndex++;
        }
        currentPathIndex++;
    }
    return { match: true, pathIndex: currentPathIndex, commandIndex: currentCommandIndex };
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function createNewSegmentGroup(segmentGroup, startIndex, commands) {
    /** @type {?} */
    const paths = segmentGroup.segments.slice(0, startIndex);
    /** @type {?} */
    let i = 0;
    while (i < commands.length) {
        if (typeof commands[i] === 'object' && commands[i].outlets !== undefined) {
            /** @type {?} */
            const children = createNewSegmentChildren(commands[i].outlets);
            return new UrlSegmentGroup(paths, children);
        }
        // if we start with an object literal, we need to reuse the path part from the segment
        if (i === 0 && isMatrixParams(commands[0])) {
            /** @type {?} */
            const p = segmentGroup.segments[startIndex];
            paths.push(new UrlSegment(p.path, commands[0]));
            i++;
            continue;
        }
        /** @type {?} */
        const curr = getPath(commands[i]);
        /** @type {?} */
        const next = (i < commands.length - 1) ? commands[i + 1] : null;
        if (curr && next && isMatrixParams(next)) {
            paths.push(new UrlSegment(curr, stringify(next)));
            i += 2;
        }
        else {
            paths.push(new UrlSegment(curr, {}));
            i++;
        }
    }
    return new UrlSegmentGroup(paths, {});
}
/**
 * @param {?} outlets
 * @return {?}
 */
function createNewSegmentChildren(outlets) {
    /** @type {?} */
    const children = {};
    forEach(outlets, (/**
     * @param {?} commands
     * @param {?} outlet
     * @return {?}
     */
    (commands, outlet) => {
        if (commands !== null) {
            children[outlet] = createNewSegmentGroup(new UrlSegmentGroup([], {}), 0, commands);
        }
    }));
    return children;
}
/**
 * @param {?} params
 * @return {?}
 */
function stringify(params) {
    /** @type {?} */
    const res = {};
    forEach(params, (/**
     * @param {?} v
     * @param {?} k
     * @return {?}
     */
    (v, k) => res[k] = `${v}`));
    return res;
}
/**
 * @param {?} path
 * @param {?} params
 * @param {?} segment
 * @return {?}
 */
function compare(path, params, segment) {
    return path == segment.path && shallowEqual(params, segment.parameters);
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const activateRoutes = (/**
 * @param {?} rootContexts
 * @param {?} routeReuseStrategy
 * @param {?} forwardEvent
 * @return {?}
 */
(rootContexts, routeReuseStrategy, forwardEvent) => map((/**
 * @param {?} t
 * @return {?}
 */
t => {
    new ActivateRoutes(routeReuseStrategy, (/** @type {?} */ (t.targetRouterState)), t.currentRouterState, forwardEvent)
        .activate(rootContexts);
    return t;
})));
class ActivateRoutes {
    /**
     * @param {?} routeReuseStrategy
     * @param {?} futureState
     * @param {?} currState
     * @param {?} forwardEvent
     */
    constructor(routeReuseStrategy, futureState, currState, forwardEvent) {
        this.routeReuseStrategy = routeReuseStrategy;
        this.futureState = futureState;
        this.currState = currState;
        this.forwardEvent = forwardEvent;
    }
    /**
     * @param {?} parentContexts
     * @return {?}
     */
    activate(parentContexts) {
        /** @type {?} */
        const futureRoot = this.futureState._root;
        /** @type {?} */
        const currRoot = this.currState ? this.currState._root : null;
        this.deactivateChildRoutes(futureRoot, currRoot, parentContexts);
        advanceActivatedRoute(this.futureState.root);
        this.activateChildRoutes(futureRoot, currRoot, parentContexts);
    }
    // De-activate the child route that are not re-used for the future state
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} contexts
     * @return {?}
     */
    deactivateChildRoutes(futureNode, currNode, contexts) {
        /** @type {?} */
        const children = nodeChildrenAsMap(currNode);
        // Recurse on the routes active in the future state to de-activate deeper children
        futureNode.children.forEach((/**
         * @param {?} futureChild
         * @return {?}
         */
        futureChild => {
            /** @type {?} */
            const childOutletName = futureChild.value.outlet;
            this.deactivateRoutes(futureChild, children[childOutletName], contexts);
            delete children[childOutletName];
        }));
        // De-activate the routes that will not be re-used
        forEach(children, (/**
         * @param {?} v
         * @param {?} childName
         * @return {?}
         */
        (v, childName) => {
            this.deactivateRouteAndItsChildren(v, contexts);
        }));
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} parentContext
     * @return {?}
     */
    deactivateRoutes(futureNode, currNode, parentContext) {
        /** @type {?} */
        const future = futureNode.value;
        /** @type {?} */
        const curr = currNode ? currNode.value : null;
        if (future === curr) {
            // Reusing the node, check to see if the children need to be de-activated
            if (future.component) {
                // If we have a normal route, we need to go through an outlet.
                /** @type {?} */
                const context = parentContext.getContext(future.outlet);
                if (context) {
                    this.deactivateChildRoutes(futureNode, currNode, context.children);
                }
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.deactivateChildRoutes(futureNode, currNode, parentContext);
            }
        }
        else {
            if (curr) {
                // Deactivate the current route which will not be re-used
                this.deactivateRouteAndItsChildren(currNode, parentContext);
            }
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    deactivateRouteAndItsChildren(route, parentContexts) {
        if (this.routeReuseStrategy.shouldDetach(route.value.snapshot)) {
            this.detachAndStoreRouteSubtree(route, parentContexts);
        }
        else {
            this.deactivateRouteAndOutlet(route, parentContexts);
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    detachAndStoreRouteSubtree(route, parentContexts) {
        /** @type {?} */
        const context = parentContexts.getContext(route.value.outlet);
        if (context && context.outlet) {
            /** @type {?} */
            const componentRef = context.outlet.detach();
            /** @type {?} */
            const contexts = context.children.onOutletDeactivated();
            this.routeReuseStrategy.store(route.value.snapshot, { componentRef, route, contexts });
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    deactivateRouteAndOutlet(route, parentContexts) {
        /** @type {?} */
        const context = parentContexts.getContext(route.value.outlet);
        if (context) {
            /** @type {?} */
            const children = nodeChildrenAsMap(route);
            /** @type {?} */
            const contexts = route.value.component ? context.children : parentContexts;
            forEach(children, (/**
             * @param {?} v
             * @param {?} k
             * @return {?}
             */
            (v, k) => this.deactivateRouteAndItsChildren(v, contexts)));
            if (context.outlet) {
                // Destroy the component
                context.outlet.deactivate();
                // Destroy the contexts for all the outlets that were in the component
                context.children.onOutletDeactivated();
            }
        }
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} contexts
     * @return {?}
     */
    activateChildRoutes(futureNode, currNode, contexts) {
        /** @type {?} */
        const children = nodeChildrenAsMap(currNode);
        futureNode.children.forEach((/**
         * @param {?} c
         * @return {?}
         */
        c => {
            this.activateRoutes(c, children[c.value.outlet], contexts);
            this.forwardEvent(new ActivationEnd(c.value.snapshot));
        }));
        if (futureNode.children.length) {
            this.forwardEvent(new ChildActivationEnd(futureNode.value.snapshot));
        }
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} parentContexts
     * @return {?}
     */
    activateRoutes(futureNode, currNode, parentContexts) {
        /** @type {?} */
        const future = futureNode.value;
        /** @type {?} */
        const curr = currNode ? currNode.value : null;
        advanceActivatedRoute(future);
        // reusing the node
        if (future === curr) {
            if (future.component) {
                // If we have a normal route, we need to go through an outlet.
                /** @type {?} */
                const context = parentContexts.getOrCreateContext(future.outlet);
                this.activateChildRoutes(futureNode, currNode, context.children);
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.activateChildRoutes(futureNode, currNode, parentContexts);
            }
        }
        else {
            if (future.component) {
                // if we have a normal route, we need to place the component into the outlet and recurse.
                /** @type {?} */
                const context = parentContexts.getOrCreateContext(future.outlet);
                if (this.routeReuseStrategy.shouldAttach(future.snapshot)) {
                    /** @type {?} */
                    const stored = ((/** @type {?} */ (this.routeReuseStrategy.retrieve(future.snapshot))));
                    this.routeReuseStrategy.store(future.snapshot, null);
                    context.children.onOutletReAttached(stored.contexts);
                    context.attachRef = stored.componentRef;
                    context.route = stored.route.value;
                    if (context.outlet) {
                        // Attach right away when the outlet has already been instantiated
                        // Otherwise attach from `RouterOutlet.ngOnInit` when it is instantiated
                        context.outlet.attach(stored.componentRef, stored.route.value);
                    }
                    advanceActivatedRouteNodeAndItsChildren(stored.route);
                }
                else {
                    /** @type {?} */
                    const config = parentLoadedConfig(future.snapshot);
                    /** @type {?} */
                    const cmpFactoryResolver = config ? config.module.componentFactoryResolver : null;
                    context.attachRef = null;
                    context.route = future;
                    context.resolver = cmpFactoryResolver;
                    if (context.outlet) {
                        // Activate the outlet when it has already been instantiated
                        // Otherwise it will get activated from its `ngOnInit` when instantiated
                        context.outlet.activateWith(future, cmpFactoryResolver);
                    }
                    this.activateChildRoutes(futureNode, null, context.children);
                }
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.activateChildRoutes(futureNode, null, parentContexts);
            }
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.routeReuseStrategy;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.futureState;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.currState;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.forwardEvent;
}
/**
 * @param {?} node
 * @return {?}
 */
function advanceActivatedRouteNodeAndItsChildren(node) {
    advanceActivatedRoute(node.value);
    node.children.forEach(advanceActivatedRouteNodeAndItsChildren);
}
/**
 * @param {?} snapshot
 * @return {?}
 */
function parentLoadedConfig(snapshot) {
    for (let s = snapshot.parent; s; s = s.parent) {
        /** @type {?} */
        const route = s.routeConfig;
        if (route && route._loadedConfig)
            return route._loadedConfig;
        if (route && route.component)
            return null;
    }
    return null;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Simple function check, but generic so type inference will flow. Example:
 *
 * function product(a: number, b: number) {
 *   return a * b;
 * }
 *
 * if (isFunction<product>(fn)) {
 *   return fn(1, 2);
 * } else {
 *   throw "Must provide the `product` function";
 * }
 * @template T
 * @param {?} v
 * @return {?}
 */
function isFunction(v) {
    return typeof v === 'function';
}
/**
 * @param {?} v
 * @return {?}
 */
function isBoolean(v) {
    return typeof v === 'boolean';
}
/**
 * @param {?} v
 * @return {?}
 */
function isUrlTree(v) {
    return v instanceof UrlTree;
}
/**
 * @param {?} guard
 * @return {?}
 */
function isCanLoad(guard) {
    return guard && isFunction(guard.canLoad);
}
/**
 * @param {?} guard
 * @return {?}
 */
function isCanActivate(guard) {
    return guard && isFunction(guard.canActivate);
}
/**
 * @param {?} guard
 * @return {?}
 */
function isCanActivateChild(guard) {
    return guard && isFunction(guard.canActivateChild);
}
/**
 * @template T
 * @param {?} guard
 * @return {?}
 */
function isCanDeactivate(guard) {
    return guard && isFunction(guard.canDeactivate);
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class NoMatch {
    /**
     * @param {?=} segmentGroup
     */
    constructor(segmentGroup) { this.segmentGroup = segmentGroup || null; }
}
if (false) {
    /** @type {?} */
    NoMatch.prototype.segmentGroup;
}
class AbsoluteRedirect {
    /**
     * @param {?} urlTree
     */
    constructor(urlTree) {
        this.urlTree = urlTree;
    }
}
if (false) {
    /** @type {?} */
    AbsoluteRedirect.prototype.urlTree;
}
/**
 * @param {?} segmentGroup
 * @return {?}
 */
function noMatch(segmentGroup) {
    return new Observable((/**
     * @param {?} obs
     * @return {?}
     */
    (obs) => obs.error(new NoMatch(segmentGroup))));
}
/**
 * @param {?} newTree
 * @return {?}
 */
function absoluteRedirect(newTree) {
    return new Observable((/**
     * @param {?} obs
     * @return {?}
     */
    (obs) => obs.error(new AbsoluteRedirect(newTree))));
}
/**
 * @param {?} redirectTo
 * @return {?}
 */
function namedOutletsRedirect(redirectTo) {
    return new Observable((/**
     * @param {?} obs
     * @return {?}
     */
    (obs) => obs.error(new Error(`Only absolute redirects can have named outlets. redirectTo: '${redirectTo}'`))));
}
/**
 * @param {?} route
 * @return {?}
 */
function canLoadFails(route) {
    return new Observable((/**
     * @param {?} obs
     * @return {?}
     */
    (obs) => obs.error(navigationCancelingError(`Cannot load children because the guard of the route "path: '${route.path}'" returned false`))));
}
/**
 * Returns the `UrlTree` with the redirection applied.
 *
 * Lazy modules are loaded along the way.
 * @param {?} moduleInjector
 * @param {?} configLoader
 * @param {?} urlSerializer
 * @param {?} urlTree
 * @param {?} config
 * @return {?}
 */
function applyRedirects(moduleInjector, configLoader, urlSerializer, urlTree, config) {
    return new ApplyRedirects(moduleInjector, configLoader, urlSerializer, urlTree, config).apply();
}
class ApplyRedirects {
    /**
     * @param {?} moduleInjector
     * @param {?} configLoader
     * @param {?} urlSerializer
     * @param {?} urlTree
     * @param {?} config
     */
    constructor(moduleInjector, configLoader, urlSerializer, urlTree, config) {
        this.configLoader = configLoader;
        this.urlSerializer = urlSerializer;
        this.urlTree = urlTree;
        this.config = config;
        this.allowRedirects = true;
        this.ngModule = moduleInjector.get(NgModuleRef);
    }
    /**
     * @return {?}
     */
    apply() {
        /** @type {?} */
        const expanded$ = this.expandSegmentGroup(this.ngModule, this.config, this.urlTree.root, PRIMARY_OUTLET);
        /** @type {?} */
        const urlTrees$ = expanded$.pipe(map((/**
         * @param {?} rootSegmentGroup
         * @return {?}
         */
        (rootSegmentGroup) => this.createUrlTree(rootSegmentGroup, this.urlTree.queryParams, (/** @type {?} */ (this.urlTree.fragment))))));
        return urlTrees$.pipe(catchError((/**
         * @param {?} e
         * @return {?}
         */
        (e) => {
            if (e instanceof AbsoluteRedirect) {
                // after an absolute redirect we do not apply any more redirects!
                this.allowRedirects = false;
                // we need to run matching, so we can fetch all lazy-loaded modules
                return this.match(e.urlTree);
            }
            if (e instanceof NoMatch) {
                throw this.noMatchError(e);
            }
            throw e;
        })));
    }
    /**
     * @private
     * @param {?} tree
     * @return {?}
     */
    match(tree) {
        /** @type {?} */
        const expanded$ = this.expandSegmentGroup(this.ngModule, this.config, tree.root, PRIMARY_OUTLET);
        /** @type {?} */
        const mapped$ = expanded$.pipe(map((/**
         * @param {?} rootSegmentGroup
         * @return {?}
         */
        (rootSegmentGroup) => this.createUrlTree(rootSegmentGroup, tree.queryParams, (/** @type {?} */ (tree.fragment))))));
        return mapped$.pipe(catchError((/**
         * @param {?} e
         * @return {?}
         */
        (e) => {
            if (e instanceof NoMatch) {
                throw this.noMatchError(e);
            }
            throw e;
        })));
    }
    /**
     * @private
     * @param {?} e
     * @return {?}
     */
    noMatchError(e) {
        return new Error(`Cannot match any routes. URL Segment: '${e.segmentGroup}'`);
    }
    /**
     * @private
     * @param {?} rootCandidate
     * @param {?} queryParams
     * @param {?} fragment
     * @return {?}
     */
    createUrlTree(rootCandidate, queryParams, fragment) {
        /** @type {?} */
        const root = rootCandidate.segments.length > 0 ?
            new UrlSegmentGroup([], { [PRIMARY_OUTLET]: rootCandidate }) :
            rootCandidate;
        return new UrlTree(root, queryParams, fragment);
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} routes
     * @param {?} segmentGroup
     * @param {?} outlet
     * @return {?}
     */
    expandSegmentGroup(ngModule, routes, segmentGroup, outlet) {
        if (segmentGroup.segments.length === 0 && segmentGroup.hasChildren()) {
            return this.expandChildren(ngModule, routes, segmentGroup)
                .pipe(map((/**
             * @param {?} children
             * @return {?}
             */
            (children) => new UrlSegmentGroup([], children))));
        }
        return this.expandSegment(ngModule, segmentGroup, routes, segmentGroup.segments, outlet, true);
    }
    // Recursively expand segment groups for all the child outlets
    /**
     * @private
     * @param {?} ngModule
     * @param {?} routes
     * @param {?} segmentGroup
     * @return {?}
     */
    expandChildren(ngModule, routes, segmentGroup) {
        return waitForMap(segmentGroup.children, (/**
         * @param {?} childOutlet
         * @param {?} child
         * @return {?}
         */
        (childOutlet, child) => this.expandSegmentGroup(ngModule, routes, child, childOutlet)));
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} segmentGroup
     * @param {?} routes
     * @param {?} segments
     * @param {?} outlet
     * @param {?} allowRedirects
     * @return {?}
     */
    expandSegment(ngModule, segmentGroup, routes, segments, outlet, allowRedirects) {
        return of(...routes).pipe(map((/**
         * @param {?} r
         * @return {?}
         */
        (r) => {
            /** @type {?} */
            const expanded$ = this.expandSegmentAgainstRoute(ngModule, segmentGroup, routes, r, segments, outlet, allowRedirects);
            return expanded$.pipe(catchError((/**
             * @param {?} e
             * @return {?}
             */
            (e) => {
                if (e instanceof NoMatch) {
                    // TODO(i): this return type doesn't match the declared Observable<UrlSegmentGroup> -
                    // talk to Jason
                    return (/** @type {?} */ (of(null)));
                }
                throw e;
            })));
        })), concatAll(), first((/**
         * @param {?} s
         * @return {?}
         */
        (s) => !!s)), catchError((/**
         * @param {?} e
         * @param {?} _
         * @return {?}
         */
        (e, _) => {
            if (e instanceof EmptyError || e.name === 'EmptyError') {
                if (this.noLeftoversInUrl(segmentGroup, segments, outlet)) {
                    return of(new UrlSegmentGroup([], {}));
                }
                throw new NoMatch(segmentGroup);
            }
            throw e;
        })));
    }
    /**
     * @private
     * @param {?} segmentGroup
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    noLeftoversInUrl(segmentGroup, segments, outlet) {
        return segments.length === 0 && !segmentGroup.children[outlet];
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} segmentGroup
     * @param {?} routes
     * @param {?} route
     * @param {?} paths
     * @param {?} outlet
     * @param {?} allowRedirects
     * @return {?}
     */
    expandSegmentAgainstRoute(ngModule, segmentGroup, routes, route, paths, outlet, allowRedirects) {
        if (getOutlet(route) !== outlet) {
            return noMatch(segmentGroup);
        }
        if (route.redirectTo === undefined) {
            return this.matchSegmentAgainstRoute(ngModule, segmentGroup, route, paths);
        }
        if (allowRedirects && this.allowRedirects) {
            return this.expandSegmentAgainstRouteUsingRedirect(ngModule, segmentGroup, routes, route, paths, outlet);
        }
        return noMatch(segmentGroup);
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} segmentGroup
     * @param {?} routes
     * @param {?} route
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    expandSegmentAgainstRouteUsingRedirect(ngModule, segmentGroup, routes, route, segments, outlet) {
        if (route.path === '**') {
            return this.expandWildCardWithParamsAgainstRouteUsingRedirect(ngModule, routes, route, outlet);
        }
        return this.expandRegularSegmentAgainstRouteUsingRedirect(ngModule, segmentGroup, routes, route, segments, outlet);
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} routes
     * @param {?} route
     * @param {?} outlet
     * @return {?}
     */
    expandWildCardWithParamsAgainstRouteUsingRedirect(ngModule, routes, route, outlet) {
        /** @type {?} */
        const newTree = this.applyRedirectCommands([], (/** @type {?} */ (route.redirectTo)), {});
        if ((/** @type {?} */ (route.redirectTo)).startsWith('/')) {
            return absoluteRedirect(newTree);
        }
        return this.lineralizeSegments(route, newTree).pipe(mergeMap((/**
         * @param {?} newSegments
         * @return {?}
         */
        (newSegments) => {
            /** @type {?} */
            const group = new UrlSegmentGroup(newSegments, {});
            return this.expandSegment(ngModule, group, routes, newSegments, outlet, false);
        })));
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} segmentGroup
     * @param {?} routes
     * @param {?} route
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    expandRegularSegmentAgainstRouteUsingRedirect(ngModule, segmentGroup, routes, route, segments, outlet) {
        const { matched, consumedSegments, lastChild, positionalParamSegments } = match(segmentGroup, route, segments);
        if (!matched)
            return noMatch(segmentGroup);
        /** @type {?} */
        const newTree = this.applyRedirectCommands(consumedSegments, (/** @type {?} */ (route.redirectTo)), (/** @type {?} */ (positionalParamSegments)));
        if ((/** @type {?} */ (route.redirectTo)).startsWith('/')) {
            return absoluteRedirect(newTree);
        }
        return this.lineralizeSegments(route, newTree).pipe(mergeMap((/**
         * @param {?} newSegments
         * @return {?}
         */
        (newSegments) => {
            return this.expandSegment(ngModule, segmentGroup, routes, newSegments.concat(segments.slice(lastChild)), outlet, false);
        })));
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} rawSegmentGroup
     * @param {?} route
     * @param {?} segments
     * @return {?}
     */
    matchSegmentAgainstRoute(ngModule, rawSegmentGroup, route, segments) {
        if (route.path === '**') {
            if (route.loadChildren) {
                return this.configLoader.load(ngModule.injector, route)
                    .pipe(map((/**
                 * @param {?} cfg
                 * @return {?}
                 */
                (cfg) => {
                    route._loadedConfig = cfg;
                    return new UrlSegmentGroup(segments, {});
                })));
            }
            return of(new UrlSegmentGroup(segments, {}));
        }
        const { matched, consumedSegments, lastChild } = match(rawSegmentGroup, route, segments);
        if (!matched)
            return noMatch(rawSegmentGroup);
        /** @type {?} */
        const rawSlicedSegments = segments.slice(lastChild);
        /** @type {?} */
        const childConfig$ = this.getChildConfig(ngModule, route, segments);
        return childConfig$.pipe(mergeMap((/**
         * @param {?} routerConfig
         * @return {?}
         */
        (routerConfig) => {
            /** @type {?} */
            const childModule = routerConfig.module;
            /** @type {?} */
            const childConfig = routerConfig.routes;
            const { segmentGroup, slicedSegments } = split(rawSegmentGroup, consumedSegments, rawSlicedSegments, childConfig);
            if (slicedSegments.length === 0 && segmentGroup.hasChildren()) {
                /** @type {?} */
                const expanded$ = this.expandChildren(childModule, childConfig, segmentGroup);
                return expanded$.pipe(map((/**
                 * @param {?} children
                 * @return {?}
                 */
                (children) => new UrlSegmentGroup(consumedSegments, children))));
            }
            if (childConfig.length === 0 && slicedSegments.length === 0) {
                return of(new UrlSegmentGroup(consumedSegments, {}));
            }
            /** @type {?} */
            const expanded$ = this.expandSegment(childModule, segmentGroup, childConfig, slicedSegments, PRIMARY_OUTLET, true);
            return expanded$.pipe(map((/**
             * @param {?} cs
             * @return {?}
             */
            (cs) => new UrlSegmentGroup(consumedSegments.concat(cs.segments), cs.children))));
        })));
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} route
     * @param {?} segments
     * @return {?}
     */
    getChildConfig(ngModule, route, segments) {
        if (route.children) {
            // The children belong to the same module
            return of(new LoadedRouterConfig(route.children, ngModule));
        }
        if (route.loadChildren) {
            // lazy children belong to the loaded module
            if (route._loadedConfig !== undefined) {
                return of(route._loadedConfig);
            }
            return runCanLoadGuard(ngModule.injector, route, segments)
                .pipe(mergeMap((/**
             * @param {?} shouldLoad
             * @return {?}
             */
            (shouldLoad) => {
                if (shouldLoad) {
                    return this.configLoader.load(ngModule.injector, route)
                        .pipe(map((/**
                     * @param {?} cfg
                     * @return {?}
                     */
                    (cfg) => {
                        route._loadedConfig = cfg;
                        return cfg;
                    })));
                }
                return canLoadFails(route);
            })));
        }
        return of(new LoadedRouterConfig([], ngModule));
    }
    /**
     * @private
     * @param {?} route
     * @param {?} urlTree
     * @return {?}
     */
    lineralizeSegments(route, urlTree) {
        /** @type {?} */
        let res = [];
        /** @type {?} */
        let c = urlTree.root;
        while (true) {
            res = res.concat(c.segments);
            if (c.numberOfChildren === 0) {
                return of(res);
            }
            if (c.numberOfChildren > 1 || !c.children[PRIMARY_OUTLET]) {
                return namedOutletsRedirect((/** @type {?} */ (route.redirectTo)));
            }
            c = c.children[PRIMARY_OUTLET];
        }
    }
    /**
     * @private
     * @param {?} segments
     * @param {?} redirectTo
     * @param {?} posParams
     * @return {?}
     */
    applyRedirectCommands(segments, redirectTo, posParams) {
        return this.applyRedirectCreatreUrlTree(redirectTo, this.urlSerializer.parse(redirectTo), segments, posParams);
    }
    /**
     * @private
     * @param {?} redirectTo
     * @param {?} urlTree
     * @param {?} segments
     * @param {?} posParams
     * @return {?}
     */
    applyRedirectCreatreUrlTree(redirectTo, urlTree, segments, posParams) {
        /** @type {?} */
        const newRoot = this.createSegmentGroup(redirectTo, urlTree.root, segments, posParams);
        return new UrlTree(newRoot, this.createQueryParams(urlTree.queryParams, this.urlTree.queryParams), urlTree.fragment);
    }
    /**
     * @private
     * @param {?} redirectToParams
     * @param {?} actualParams
     * @return {?}
     */
    createQueryParams(redirectToParams, actualParams) {
        /** @type {?} */
        const res = {};
        forEach(redirectToParams, (/**
         * @param {?} v
         * @param {?} k
         * @return {?}
         */
        (v, k) => {
            /** @type {?} */
            const copySourceValue = typeof v === 'string' && v.startsWith(':');
            if (copySourceValue) {
                /** @type {?} */
                const sourceName = v.substring(1);
                res[k] = actualParams[sourceName];
            }
            else {
                res[k] = v;
            }
        }));
        return res;
    }
    /**
     * @private
     * @param {?} redirectTo
     * @param {?} group
     * @param {?} segments
     * @param {?} posParams
     * @return {?}
     */
    createSegmentGroup(redirectTo, group, segments, posParams) {
        /** @type {?} */
        const updatedSegments = this.createSegments(redirectTo, group.segments, segments, posParams);
        /** @type {?} */
        let children = {};
        forEach(group.children, (/**
         * @param {?} child
         * @param {?} name
         * @return {?}
         */
        (child, name) => {
            children[name] = this.createSegmentGroup(redirectTo, child, segments, posParams);
        }));
        return new UrlSegmentGroup(updatedSegments, children);
    }
    /**
     * @private
     * @param {?} redirectTo
     * @param {?} redirectToSegments
     * @param {?} actualSegments
     * @param {?} posParams
     * @return {?}
     */
    createSegments(redirectTo, redirectToSegments, actualSegments, posParams) {
        return redirectToSegments.map((/**
         * @param {?} s
         * @return {?}
         */
        s => s.path.startsWith(':') ? this.findPosParam(redirectTo, s, posParams) :
            this.findOrReturn(s, actualSegments)));
    }
    /**
     * @private
     * @param {?} redirectTo
     * @param {?} redirectToUrlSegment
     * @param {?} posParams
     * @return {?}
     */
    findPosParam(redirectTo, redirectToUrlSegment, posParams) {
        /** @type {?} */
        const pos = posParams[redirectToUrlSegment.path.substring(1)];
        if (!pos)
            throw new Error(`Cannot redirect to '${redirectTo}'. Cannot find '${redirectToUrlSegment.path}'.`);
        return pos;
    }
    /**
     * @private
     * @param {?} redirectToUrlSegment
     * @param {?} actualSegments
     * @return {?}
     */
    findOrReturn(redirectToUrlSegment, actualSegments) {
        /** @type {?} */
        let idx = 0;
        for (const s of actualSegments) {
            if (s.path === redirectToUrlSegment.path) {
                actualSegments.splice(idx);
                return s;
            }
            idx++;
        }
        return redirectToUrlSegment;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.allowRedirects;
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.ngModule;
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.configLoader;
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.urlSerializer;
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.urlTree;
    /**
     * @type {?}
     * @private
     */
    ApplyRedirects.prototype.config;
}
/**
 * @param {?} moduleInjector
 * @param {?} route
 * @param {?} segments
 * @return {?}
 */
function runCanLoadGuard(moduleInjector, route, segments) {
    /** @type {?} */
    const canLoad = route.canLoad;
    if (!canLoad || canLoad.length === 0)
        return of(true);
    /** @type {?} */
    const obs = from(canLoad).pipe(map((/**
     * @param {?} injectionToken
     * @return {?}
     */
    (injectionToken) => {
        /** @type {?} */
        const guard = moduleInjector.get(injectionToken);
        /** @type {?} */
        let guardVal;
        if (isCanLoad(guard)) {
            guardVal = guard.canLoad(route, segments);
        }
        else if (isFunction(guard)) {
            guardVal = guard(route, segments);
        }
        else {
            throw new Error('Invalid CanLoad guard');
        }
        return wrapIntoObservable(guardVal);
    })));
    return obs.pipe(concatAll(), every((/**
     * @param {?} result
     * @return {?}
     */
    result => result === true)));
}
/**
 * @param {?} segmentGroup
 * @param {?} route
 * @param {?} segments
 * @return {?}
 */
function match(segmentGroup, route, segments) {
    if (route.path === '') {
        if ((route.pathMatch === 'full') && (segmentGroup.hasChildren() || segments.length > 0)) {
            return { matched: false, consumedSegments: [], lastChild: 0, positionalParamSegments: {} };
        }
        return { matched: true, consumedSegments: [], lastChild: 0, positionalParamSegments: {} };
    }
    /** @type {?} */
    const matcher = route.matcher || defaultUrlMatcher;
    /** @type {?} */
    const res = matcher(segments, segmentGroup, route);
    if (!res) {
        return {
            matched: false,
            consumedSegments: (/** @type {?} */ ([])),
            lastChild: 0,
            positionalParamSegments: {},
        };
    }
    return {
        matched: true,
        consumedSegments: (/** @type {?} */ (res.consumed)),
        lastChild: (/** @type {?} */ (res.consumed.length)),
        positionalParamSegments: (/** @type {?} */ (res.posParams)),
    };
}
/**
 * @param {?} segmentGroup
 * @param {?} consumedSegments
 * @param {?} slicedSegments
 * @param {?} config
 * @return {?}
 */
function split(segmentGroup, consumedSegments, slicedSegments, config) {
    if (slicedSegments.length > 0 &&
        containsEmptyPathRedirectsWithNamedOutlets(segmentGroup, slicedSegments, config)) {
        /** @type {?} */
        const s = new UrlSegmentGroup(consumedSegments, createChildrenForEmptySegments(config, new UrlSegmentGroup(slicedSegments, segmentGroup.children)));
        return { segmentGroup: mergeTrivialChildren(s), slicedSegments: [] };
    }
    if (slicedSegments.length === 0 &&
        containsEmptyPathRedirects(segmentGroup, slicedSegments, config)) {
        /** @type {?} */
        const s = new UrlSegmentGroup(segmentGroup.segments, addEmptySegmentsToChildrenIfNeeded(segmentGroup, slicedSegments, config, segmentGroup.children));
        return { segmentGroup: mergeTrivialChildren(s), slicedSegments };
    }
    return { segmentGroup, slicedSegments };
}
/**
 * @param {?} s
 * @return {?}
 */
function mergeTrivialChildren(s) {
    if (s.numberOfChildren === 1 && s.children[PRIMARY_OUTLET]) {
        /** @type {?} */
        const c = s.children[PRIMARY_OUTLET];
        return new UrlSegmentGroup(s.segments.concat(c.segments), c.children);
    }
    return s;
}
/**
 * @param {?} segmentGroup
 * @param {?} slicedSegments
 * @param {?} routes
 * @param {?} children
 * @return {?}
 */
function addEmptySegmentsToChildrenIfNeeded(segmentGroup, slicedSegments, routes, children) {
    /** @type {?} */
    const res = {};
    for (const r of routes) {
        if (isEmptyPathRedirect(segmentGroup, slicedSegments, r) && !children[getOutlet(r)]) {
            res[getOutlet(r)] = new UrlSegmentGroup([], {});
        }
    }
    return Object.assign({}, children, res);
}
/**
 * @param {?} routes
 * @param {?} primarySegmentGroup
 * @return {?}
 */
function createChildrenForEmptySegments(routes, primarySegmentGroup) {
    /** @type {?} */
    const res = {};
    res[PRIMARY_OUTLET] = primarySegmentGroup;
    for (const r of routes) {
        if (r.path === '' && getOutlet(r) !== PRIMARY_OUTLET) {
            res[getOutlet(r)] = new UrlSegmentGroup([], {});
        }
    }
    return res;
}
/**
 * @param {?} segmentGroup
 * @param {?} segments
 * @param {?} routes
 * @return {?}
 */
function containsEmptyPathRedirectsWithNamedOutlets(segmentGroup, segments, routes) {
    return routes.some((/**
     * @param {?} r
     * @return {?}
     */
    r => isEmptyPathRedirect(segmentGroup, segments, r) && getOutlet(r) !== PRIMARY_OUTLET));
}
/**
 * @param {?} segmentGroup
 * @param {?} segments
 * @param {?} routes
 * @return {?}
 */
function containsEmptyPathRedirects(segmentGroup, segments, routes) {
    return routes.some((/**
     * @param {?} r
     * @return {?}
     */
    r => isEmptyPathRedirect(segmentGroup, segments, r)));
}
/**
 * @param {?} segmentGroup
 * @param {?} segments
 * @param {?} r
 * @return {?}
 */
function isEmptyPathRedirect(segmentGroup, segments, r) {
    if ((segmentGroup.hasChildren() || segments.length > 0) && r.pathMatch === 'full') {
        return false;
    }
    return r.path === '' && r.redirectTo !== undefined;
}
/**
 * @param {?} route
 * @return {?}
 */
function getOutlet(route) {
    return route.outlet || PRIMARY_OUTLET;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} moduleInjector
 * @param {?} configLoader
 * @param {?} urlSerializer
 * @param {?} config
 * @return {?}
 */
function applyRedirects$1(moduleInjector, configLoader, urlSerializer, config) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(switchMap((/**
         * @param {?} t
         * @return {?}
         */
        t => applyRedirects(moduleInjector, configLoader, urlSerializer, t.extractedUrl, config)
            .pipe(map((/**
         * @param {?} urlAfterRedirects
         * @return {?}
         */
        urlAfterRedirects => (Object.assign({}, t, { urlAfterRedirects }))))))));
    });
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class CanActivate {
    /**
     * @param {?} path
     */
    constructor(path) {
        this.path = path;
        this.route = this.path[this.path.length - 1];
    }
}
if (false) {
    /** @type {?} */
    CanActivate.prototype.route;
    /** @type {?} */
    CanActivate.prototype.path;
}
class CanDeactivate {
    /**
     * @param {?} component
     * @param {?} route
     */
    constructor(component, route) {
        this.component = component;
        this.route = route;
    }
}
if (false) {
    /** @type {?} */
    CanDeactivate.prototype.component;
    /** @type {?} */
    CanDeactivate.prototype.route;
}
/**
 * @param {?} future
 * @param {?} curr
 * @param {?} parentContexts
 * @return {?}
 */
function getAllRouteGuards(future, curr, parentContexts) {
    /** @type {?} */
    const futureRoot = future._root;
    /** @type {?} */
    const currRoot = curr ? curr._root : null;
    return getChildRouteGuards(futureRoot, currRoot, parentContexts, [futureRoot.value]);
}
/**
 * @param {?} p
 * @return {?}
 */
function getCanActivateChild(p) {
    /** @type {?} */
    const canActivateChild = p.routeConfig ? p.routeConfig.canActivateChild : null;
    if (!canActivateChild || canActivateChild.length === 0)
        return null;
    return { node: p, guards: canActivateChild };
}
/**
 * @param {?} token
 * @param {?} snapshot
 * @param {?} moduleInjector
 * @return {?}
 */
function getToken(token, snapshot, moduleInjector) {
    /** @type {?} */
    const config = getClosestLoadedConfig(snapshot);
    /** @type {?} */
    const injector = config ? config.module.injector : moduleInjector;
    return injector.get(token);
}
/**
 * @param {?} snapshot
 * @return {?}
 */
function getClosestLoadedConfig(snapshot) {
    if (!snapshot)
        return null;
    for (let s = snapshot.parent; s; s = s.parent) {
        /** @type {?} */
        const route = s.routeConfig;
        if (route && route._loadedConfig)
            return route._loadedConfig;
    }
    return null;
}
/**
 * @param {?} futureNode
 * @param {?} currNode
 * @param {?} contexts
 * @param {?} futurePath
 * @param {?=} checks
 * @return {?}
 */
function getChildRouteGuards(futureNode, currNode, contexts, futurePath, checks = {
    canDeactivateChecks: [],
    canActivateChecks: []
}) {
    /** @type {?} */
    const prevChildren = nodeChildrenAsMap(currNode);
    // Process the children of the future route
    futureNode.children.forEach((/**
     * @param {?} c
     * @return {?}
     */
    c => {
        getRouteGuards(c, prevChildren[c.value.outlet], contexts, futurePath.concat([c.value]), checks);
        delete prevChildren[c.value.outlet];
    }));
    // Process any children left from the current route (not active for the future route)
    forEach(prevChildren, (/**
     * @param {?} v
     * @param {?} k
     * @return {?}
     */
    (v, k) => deactivateRouteAndItsChildren(v, (/** @type {?} */ (contexts)).getContext(k), checks)));
    return checks;
}
/**
 * @param {?} futureNode
 * @param {?} currNode
 * @param {?} parentContexts
 * @param {?} futurePath
 * @param {?=} checks
 * @return {?}
 */
function getRouteGuards(futureNode, currNode, parentContexts, futurePath, checks = {
    canDeactivateChecks: [],
    canActivateChecks: []
}) {
    /** @type {?} */
    const future = futureNode.value;
    /** @type {?} */
    const curr = currNode ? currNode.value : null;
    /** @type {?} */
    const context = parentContexts ? parentContexts.getContext(futureNode.value.outlet) : null;
    // reusing the node
    if (curr && future.routeConfig === curr.routeConfig) {
        /** @type {?} */
        const shouldRun = shouldRunGuardsAndResolvers(curr, future, (/** @type {?} */ (future.routeConfig)).runGuardsAndResolvers);
        if (shouldRun) {
            checks.canActivateChecks.push(new CanActivate(futurePath));
        }
        else {
            // we need to set the data
            future.data = curr.data;
            future._resolvedData = curr._resolvedData;
        }
        // If we have a component, we need to go through an outlet.
        if (future.component) {
            getChildRouteGuards(futureNode, currNode, context ? context.children : null, futurePath, checks);
            // if we have a componentless route, we recurse but keep the same outlet map.
        }
        else {
            getChildRouteGuards(futureNode, currNode, parentContexts, futurePath, checks);
        }
        if (shouldRun) {
            /** @type {?} */
            const component = context && context.outlet && context.outlet.component || null;
            checks.canDeactivateChecks.push(new CanDeactivate(component, curr));
        }
    }
    else {
        if (curr) {
            deactivateRouteAndItsChildren(currNode, context, checks);
        }
        checks.canActivateChecks.push(new CanActivate(futurePath));
        // If we have a component, we need to go through an outlet.
        if (future.component) {
            getChildRouteGuards(futureNode, null, context ? context.children : null, futurePath, checks);
            // if we have a componentless route, we recurse but keep the same outlet map.
        }
        else {
            getChildRouteGuards(futureNode, null, parentContexts, futurePath, checks);
        }
    }
    return checks;
}
/**
 * @param {?} curr
 * @param {?} future
 * @param {?} mode
 * @return {?}
 */
function shouldRunGuardsAndResolvers(curr, future, mode) {
    if (typeof mode === 'function') {
        return mode(curr, future);
    }
    switch (mode) {
        case 'pathParamsChange':
            return !equalPath(curr.url, future.url);
        case 'pathParamsOrQueryParamsChange':
            return !equalPath(curr.url, future.url) ||
                !shallowEqual(curr.queryParams, future.queryParams);
        case 'always':
            return true;
        case 'paramsOrQueryParamsChange':
            return !equalParamsAndUrlSegments(curr, future) ||
                !shallowEqual(curr.queryParams, future.queryParams);
        case 'paramsChange':
        default:
            return !equalParamsAndUrlSegments(curr, future);
    }
}
/**
 * @param {?} route
 * @param {?} context
 * @param {?} checks
 * @return {?}
 */
function deactivateRouteAndItsChildren(route, context, checks) {
    /** @type {?} */
    const children = nodeChildrenAsMap(route);
    /** @type {?} */
    const r = route.value;
    forEach(children, (/**
     * @param {?} node
     * @param {?} childName
     * @return {?}
     */
    (node, childName) => {
        if (!r.component) {
            deactivateRouteAndItsChildren(node, context, checks);
        }
        else if (context) {
            deactivateRouteAndItsChildren(node, context.children.getContext(childName), checks);
        }
        else {
            deactivateRouteAndItsChildren(node, null, checks);
        }
    }));
    if (!r.component) {
        checks.canDeactivateChecks.push(new CanDeactivate(null, r));
    }
    else if (context && context.outlet && context.outlet.isActivated) {
        checks.canDeactivateChecks.push(new CanDeactivate(context.outlet.component, r));
    }
    else {
        checks.canDeactivateChecks.push(new CanDeactivate(null, r));
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const INITIAL_VALUE = Symbol('INITIAL_VALUE');
/**
 * @return {?}
 */
function prioritizedGuardValue() {
    return switchMap((/**
     * @param {?} obs
     * @return {?}
     */
    obs => {
        return (/** @type {?} */ (combineLatest(...obs.map((/**
         * @param {?} o
         * @return {?}
         */
        o => o.pipe(take(1), startWith((/** @type {?} */ (INITIAL_VALUE)))))))
            .pipe(scan((/**
         * @param {?} acc
         * @param {?} list
         * @return {?}
         */
        (acc, list) => {
            /** @type {?} */
            let isPending = false;
            return list.reduce((/**
             * @param {?} innerAcc
             * @param {?} val
             * @param {?} i
             * @return {?}
             */
            (innerAcc, val, i) => {
                if (innerAcc !== INITIAL_VALUE)
                    return innerAcc;
                // Toggle pending flag if any values haven't been set yet
                if (val === INITIAL_VALUE)
                    isPending = true;
                // Any other return values are only valid if we haven't yet hit a pending call.
                // This guarantees that in the case of a guard at the bottom of the tree that
                // returns a redirect, we will wait for the higher priority guard at the top to
                // finish before performing the redirect.
                if (!isPending) {
                    // Early return when we hit a `false` value as that should always cancel
                    // navigation
                    if (val === false)
                        return val;
                    if (i === list.length - 1 || isUrlTree(val)) {
                        return val;
                    }
                }
                return innerAcc;
            }), acc);
        }), INITIAL_VALUE), filter((/**
         * @param {?} item
         * @return {?}
         */
        item => item !== INITIAL_VALUE)), map((/**
         * @param {?} item
         * @return {?}
         */
        item => isUrlTree(item) ? item : item === true)), //
        take(1))));
    }));
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} moduleInjector
 * @param {?=} forwardEvent
 * @return {?}
 */
function checkGuards(moduleInjector, forwardEvent) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            const { targetSnapshot, currentSnapshot, guards: { canActivateChecks, canDeactivateChecks } } = t;
            if (canDeactivateChecks.length === 0 && canActivateChecks.length === 0) {
                return of(Object.assign({}, t, { guardsResult: true }));
            }
            return runCanDeactivateChecks(canDeactivateChecks, (/** @type {?} */ (targetSnapshot)), currentSnapshot, moduleInjector)
                .pipe(mergeMap((/**
             * @param {?} canDeactivate
             * @return {?}
             */
            canDeactivate => {
                return canDeactivate && isBoolean(canDeactivate) ?
                    runCanActivateChecks((/** @type {?} */ (targetSnapshot)), canActivateChecks, moduleInjector, forwardEvent) :
                    of(canDeactivate);
            })), map((/**
             * @param {?} guardsResult
             * @return {?}
             */
            guardsResult => (Object.assign({}, t, { guardsResult })))));
        })));
    });
}
/**
 * @param {?} checks
 * @param {?} futureRSS
 * @param {?} currRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanDeactivateChecks(checks, futureRSS, currRSS, moduleInjector) {
    return from(checks).pipe(mergeMap((/**
     * @param {?} check
     * @return {?}
     */
    check => runCanDeactivate(check.component, check.route, currRSS, futureRSS, moduleInjector))), first((/**
     * @param {?} result
     * @return {?}
     */
    result => { return result !== true; }), (/** @type {?} */ (true))));
}
/**
 * @param {?} futureSnapshot
 * @param {?} checks
 * @param {?} moduleInjector
 * @param {?=} forwardEvent
 * @return {?}
 */
function runCanActivateChecks(futureSnapshot, checks, moduleInjector, forwardEvent) {
    return from(checks).pipe(concatMap((/**
     * @param {?} check
     * @return {?}
     */
    (check) => {
        return from([
            fireChildActivationStart(check.route.parent, forwardEvent),
            fireActivationStart(check.route, forwardEvent),
            runCanActivateChild(futureSnapshot, check.path, moduleInjector),
            runCanActivate(futureSnapshot, check.route, moduleInjector)
        ])
            .pipe(concatAll(), first((/**
         * @param {?} result
         * @return {?}
         */
        result => {
            return result !== true;
        }), (/** @type {?} */ (true))));
    })), first((/**
     * @param {?} result
     * @return {?}
     */
    result => { return result !== true; }), (/** @type {?} */ (true))));
}
/**
 * This should fire off `ActivationStart` events for each route being activated at this
 * level.
 * In other words, if you're activating `a` and `b` below, `path` will contain the
 * `ActivatedRouteSnapshot`s for both and we will fire `ActivationStart` for both. Always
 * return
 * `true` so checks continue to run.
 * @param {?} snapshot
 * @param {?=} forwardEvent
 * @return {?}
 */
function fireActivationStart(snapshot, forwardEvent) {
    if (snapshot !== null && forwardEvent) {
        forwardEvent(new ActivationStart(snapshot));
    }
    return of(true);
}
/**
 * This should fire off `ChildActivationStart` events for each route being activated at this
 * level.
 * In other words, if you're activating `a` and `b` below, `path` will contain the
 * `ActivatedRouteSnapshot`s for both and we will fire `ChildActivationStart` for both. Always
 * return
 * `true` so checks continue to run.
 * @param {?} snapshot
 * @param {?=} forwardEvent
 * @return {?}
 */
function fireChildActivationStart(snapshot, forwardEvent) {
    if (snapshot !== null && forwardEvent) {
        forwardEvent(new ChildActivationStart(snapshot));
    }
    return of(true);
}
/**
 * @param {?} futureRSS
 * @param {?} futureARS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanActivate(futureRSS, futureARS, moduleInjector) {
    /** @type {?} */
    const canActivate = futureARS.routeConfig ? futureARS.routeConfig.canActivate : null;
    if (!canActivate || canActivate.length === 0)
        return of(true);
    /** @type {?} */
    const canActivateObservables = canActivate.map((/**
     * @param {?} c
     * @return {?}
     */
    (c) => {
        return defer((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const guard = getToken(c, futureARS, moduleInjector);
            /** @type {?} */
            let observable;
            if (isCanActivate(guard)) {
                observable = wrapIntoObservable(guard.canActivate(futureARS, futureRSS));
            }
            else if (isFunction(guard)) {
                observable = wrapIntoObservable(guard(futureARS, futureRSS));
            }
            else {
                throw new Error('Invalid CanActivate guard');
            }
            return observable.pipe(first());
        }));
    }));
    return of(canActivateObservables).pipe(prioritizedGuardValue());
}
/**
 * @param {?} futureRSS
 * @param {?} path
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanActivateChild(futureRSS, path, moduleInjector) {
    /** @type {?} */
    const futureARS = path[path.length - 1];
    /** @type {?} */
    const canActivateChildGuards = path.slice(0, path.length - 1)
        .reverse()
        .map((/**
     * @param {?} p
     * @return {?}
     */
    p => getCanActivateChild(p)))
        .filter((/**
     * @param {?} _
     * @return {?}
     */
    _ => _ !== null));
    /** @type {?} */
    const canActivateChildGuardsMapped = canActivateChildGuards.map((/**
     * @param {?} d
     * @return {?}
     */
    (d) => {
        return defer((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const guardsMapped = d.guards.map((/**
             * @param {?} c
             * @return {?}
             */
            (c) => {
                /** @type {?} */
                const guard = getToken(c, d.node, moduleInjector);
                /** @type {?} */
                let observable;
                if (isCanActivateChild(guard)) {
                    observable = wrapIntoObservable(guard.canActivateChild(futureARS, futureRSS));
                }
                else if (isFunction(guard)) {
                    observable = wrapIntoObservable(guard(futureARS, futureRSS));
                }
                else {
                    throw new Error('Invalid CanActivateChild guard');
                }
                return observable.pipe(first());
            }));
            return of(guardsMapped).pipe(prioritizedGuardValue());
        }));
    }));
    return of(canActivateChildGuardsMapped).pipe(prioritizedGuardValue());
}
/**
 * @param {?} component
 * @param {?} currARS
 * @param {?} currRSS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanDeactivate(component, currARS, currRSS, futureRSS, moduleInjector) {
    /** @type {?} */
    const canDeactivate = currARS && currARS.routeConfig ? currARS.routeConfig.canDeactivate : null;
    if (!canDeactivate || canDeactivate.length === 0)
        return of(true);
    /** @type {?} */
    const canDeactivateObservables = canDeactivate.map((/**
     * @param {?} c
     * @return {?}
     */
    (c) => {
        /** @type {?} */
        const guard = getToken(c, currARS, moduleInjector);
        /** @type {?} */
        let observable;
        if (isCanDeactivate(guard)) {
            observable =
                wrapIntoObservable(guard.canDeactivate((/** @type {?} */ (component)), currARS, currRSS, futureRSS));
        }
        else if (isFunction(guard)) {
            observable = wrapIntoObservable(guard(component, currARS, currRSS, futureRSS));
        }
        else {
            throw new Error('Invalid CanDeactivate guard');
        }
        return observable.pipe(first());
    }));
    return of(canDeactivateObservables).pipe(prioritizedGuardValue());
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class NoMatch$1 {
}
/**
 * @param {?} rootComponentType
 * @param {?} config
 * @param {?} urlTree
 * @param {?} url
 * @param {?=} paramsInheritanceStrategy
 * @param {?=} relativeLinkResolution
 * @return {?}
 */
function recognize(rootComponentType, config, urlTree, url, paramsInheritanceStrategy = 'emptyOnly', relativeLinkResolution = 'legacy') {
    return new Recognizer(rootComponentType, config, urlTree, url, paramsInheritanceStrategy, relativeLinkResolution)
        .recognize();
}
class Recognizer {
    /**
     * @param {?} rootComponentType
     * @param {?} config
     * @param {?} urlTree
     * @param {?} url
     * @param {?} paramsInheritanceStrategy
     * @param {?} relativeLinkResolution
     */
    constructor(rootComponentType, config, urlTree, url, paramsInheritanceStrategy, relativeLinkResolution) {
        this.rootComponentType = rootComponentType;
        this.config = config;
        this.urlTree = urlTree;
        this.url = url;
        this.paramsInheritanceStrategy = paramsInheritanceStrategy;
        this.relativeLinkResolution = relativeLinkResolution;
    }
    /**
     * @return {?}
     */
    recognize() {
        try {
            /** @type {?} */
            const rootSegmentGroup = split$1(this.urlTree.root, [], [], this.config, this.relativeLinkResolution).segmentGroup;
            /** @type {?} */
            const children = this.processSegmentGroup(this.config, rootSegmentGroup, PRIMARY_OUTLET);
            /** @type {?} */
            const root = new ActivatedRouteSnapshot([], Object.freeze({}), Object.freeze(Object.assign({}, this.urlTree.queryParams)), (/** @type {?} */ (this.urlTree.fragment)), {}, PRIMARY_OUTLET, this.rootComponentType, null, this.urlTree.root, -1, {});
            /** @type {?} */
            const rootNode = new TreeNode(root, children);
            /** @type {?} */
            const routeState = new RouterStateSnapshot(this.url, rootNode);
            this.inheritParamsAndData(routeState._root);
            return of(routeState);
        }
        catch (e) {
            return new Observable((/**
             * @param {?} obs
             * @return {?}
             */
            (obs) => obs.error(e)));
        }
    }
    /**
     * @param {?} routeNode
     * @return {?}
     */
    inheritParamsAndData(routeNode) {
        /** @type {?} */
        const route = routeNode.value;
        /** @type {?} */
        const i = inheritedParamsDataResolve(route, this.paramsInheritanceStrategy);
        route.params = Object.freeze(i.params);
        route.data = Object.freeze(i.data);
        routeNode.children.forEach((/**
         * @param {?} n
         * @return {?}
         */
        n => this.inheritParamsAndData(n)));
    }
    /**
     * @param {?} config
     * @param {?} segmentGroup
     * @param {?} outlet
     * @return {?}
     */
    processSegmentGroup(config, segmentGroup, outlet) {
        if (segmentGroup.segments.length === 0 && segmentGroup.hasChildren()) {
            return this.processChildren(config, segmentGroup);
        }
        return this.processSegment(config, segmentGroup, segmentGroup.segments, outlet);
    }
    /**
     * @param {?} config
     * @param {?} segmentGroup
     * @return {?}
     */
    processChildren(config, segmentGroup) {
        /** @type {?} */
        const children = mapChildrenIntoArray(segmentGroup, (/**
         * @param {?} child
         * @param {?} childOutlet
         * @return {?}
         */
        (child, childOutlet) => this.processSegmentGroup(config, child, childOutlet)));
        checkOutletNameUniqueness(children);
        sortActivatedRouteSnapshots(children);
        return children;
    }
    /**
     * @param {?} config
     * @param {?} segmentGroup
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    processSegment(config, segmentGroup, segments, outlet) {
        for (const r of config) {
            try {
                return this.processSegmentAgainstRoute(r, segmentGroup, segments, outlet);
            }
            catch (e) {
                if (!(e instanceof NoMatch$1))
                    throw e;
            }
        }
        if (this.noLeftoversInUrl(segmentGroup, segments, outlet)) {
            return [];
        }
        throw new NoMatch$1();
    }
    /**
     * @private
     * @param {?} segmentGroup
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    noLeftoversInUrl(segmentGroup, segments, outlet) {
        return segments.length === 0 && !segmentGroup.children[outlet];
    }
    /**
     * @param {?} route
     * @param {?} rawSegment
     * @param {?} segments
     * @param {?} outlet
     * @return {?}
     */
    processSegmentAgainstRoute(route, rawSegment, segments, outlet) {
        if (route.redirectTo)
            throw new NoMatch$1();
        if ((route.outlet || PRIMARY_OUTLET) !== outlet)
            throw new NoMatch$1();
        /** @type {?} */
        let snapshot;
        /** @type {?} */
        let consumedSegments = [];
        /** @type {?} */
        let rawSlicedSegments = [];
        if (route.path === '**') {
            /** @type {?} */
            const params = segments.length > 0 ? (/** @type {?} */ (last(segments))).parameters : {};
            snapshot = new ActivatedRouteSnapshot(segments, params, Object.freeze(Object.assign({}, this.urlTree.queryParams)), (/** @type {?} */ (this.urlTree.fragment)), getData(route), outlet, (/** @type {?} */ (route.component)), route, getSourceSegmentGroup(rawSegment), getPathIndexShift(rawSegment) + segments.length, getResolve(route));
        }
        else {
            /** @type {?} */
            const result = match$1(rawSegment, route, segments);
            consumedSegments = result.consumedSegments;
            rawSlicedSegments = segments.slice(result.lastChild);
            snapshot = new ActivatedRouteSnapshot(consumedSegments, result.parameters, Object.freeze(Object.assign({}, this.urlTree.queryParams)), (/** @type {?} */ (this.urlTree.fragment)), getData(route), outlet, (/** @type {?} */ (route.component)), route, getSourceSegmentGroup(rawSegment), getPathIndexShift(rawSegment) + consumedSegments.length, getResolve(route));
        }
        /** @type {?} */
        const childConfig = getChildConfig(route);
        const { segmentGroup, slicedSegments } = split$1(rawSegment, consumedSegments, rawSlicedSegments, childConfig, this.relativeLinkResolution);
        if (slicedSegments.length === 0 && segmentGroup.hasChildren()) {
            /** @type {?} */
            const children = this.processChildren(childConfig, segmentGroup);
            return [new TreeNode(snapshot, children)];
        }
        if (childConfig.length === 0 && slicedSegments.length === 0) {
            return [new TreeNode(snapshot, [])];
        }
        /** @type {?} */
        const children = this.processSegment(childConfig, segmentGroup, slicedSegments, PRIMARY_OUTLET);
        return [new TreeNode(snapshot, children)];
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.rootComponentType;
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.config;
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.urlTree;
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.url;
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.paramsInheritanceStrategy;
    /**
     * @type {?}
     * @private
     */
    Recognizer.prototype.relativeLinkResolution;
}
/**
 * @param {?} nodes
 * @return {?}
 */
function sortActivatedRouteSnapshots(nodes) {
    nodes.sort((/**
     * @param {?} a
     * @param {?} b
     * @return {?}
     */
    (a, b) => {
        if (a.value.outlet === PRIMARY_OUTLET)
            return -1;
        if (b.value.outlet === PRIMARY_OUTLET)
            return 1;
        return a.value.outlet.localeCompare(b.value.outlet);
    }));
}
/**
 * @param {?} route
 * @return {?}
 */
function getChildConfig(route) {
    if (route.children) {
        return route.children;
    }
    if (route.loadChildren) {
        return (/** @type {?} */ (route._loadedConfig)).routes;
    }
    return [];
}
/**
 * @record
 */
function MatchResult() { }
if (false) {
    /** @type {?} */
    MatchResult.prototype.consumedSegments;
    /** @type {?} */
    MatchResult.prototype.lastChild;
    /** @type {?} */
    MatchResult.prototype.parameters;
}
/**
 * @param {?} segmentGroup
 * @param {?} route
 * @param {?} segments
 * @return {?}
 */
function match$1(segmentGroup, route, segments) {
    if (route.path === '') {
        if (route.pathMatch === 'full' && (segmentGroup.hasChildren() || segments.length > 0)) {
            throw new NoMatch$1();
        }
        return { consumedSegments: [], lastChild: 0, parameters: {} };
    }
    /** @type {?} */
    const matcher = route.matcher || defaultUrlMatcher;
    /** @type {?} */
    const res = matcher(segments, segmentGroup, route);
    if (!res)
        throw new NoMatch$1();
    /** @type {?} */
    const posParams = {};
    forEach((/** @type {?} */ (res.posParams)), (/**
     * @param {?} v
     * @param {?} k
     * @return {?}
     */
    (v, k) => { posParams[k] = v.path; }));
    /** @type {?} */
    const parameters = res.consumed.length > 0 ? Object.assign({}, posParams, res.consumed[res.consumed.length - 1].parameters) :
        posParams;
    return { consumedSegments: res.consumed, lastChild: res.consumed.length, parameters };
}
/**
 * @param {?} nodes
 * @return {?}
 */
function checkOutletNameUniqueness(nodes) {
    /** @type {?} */
    const names = {};
    nodes.forEach((/**
     * @param {?} n
     * @return {?}
     */
    n => {
        /** @type {?} */
        const routeWithSameOutletName = names[n.value.outlet];
        if (routeWithSameOutletName) {
            /** @type {?} */
            const p = routeWithSameOutletName.url.map((/**
             * @param {?} s
             * @return {?}
             */
            s => s.toString())).join('/');
            /** @type {?} */
            const c = n.value.url.map((/**
             * @param {?} s
             * @return {?}
             */
            s => s.toString())).join('/');
            throw new Error(`Two segments cannot have the same outlet name: '${p}' and '${c}'.`);
        }
        names[n.value.outlet] = n.value;
    }));
}
/**
 * @param {?} segmentGroup
 * @return {?}
 */
function getSourceSegmentGroup(segmentGroup) {
    /** @type {?} */
    let s = segmentGroup;
    while (s._sourceSegment) {
        s = s._sourceSegment;
    }
    return s;
}
/**
 * @param {?} segmentGroup
 * @return {?}
 */
function getPathIndexShift(segmentGroup) {
    /** @type {?} */
    let s = segmentGroup;
    /** @type {?} */
    let res = (s._segmentIndexShift ? s._segmentIndexShift : 0);
    while (s._sourceSegment) {
        s = s._sourceSegment;
        res += (s._segmentIndexShift ? s._segmentIndexShift : 0);
    }
    return res - 1;
}
/**
 * @param {?} segmentGroup
 * @param {?} consumedSegments
 * @param {?} slicedSegments
 * @param {?} config
 * @param {?} relativeLinkResolution
 * @return {?}
 */
function split$1(segmentGroup, consumedSegments, slicedSegments, config, relativeLinkResolution) {
    if (slicedSegments.length > 0 &&
        containsEmptyPathMatchesWithNamedOutlets(segmentGroup, slicedSegments, config)) {
        /** @type {?} */
        const s = new UrlSegmentGroup(consumedSegments, createChildrenForEmptyPaths(segmentGroup, consumedSegments, config, new UrlSegmentGroup(slicedSegments, segmentGroup.children)));
        s._sourceSegment = segmentGroup;
        s._segmentIndexShift = consumedSegments.length;
        return { segmentGroup: s, slicedSegments: [] };
    }
    if (slicedSegments.length === 0 &&
        containsEmptyPathMatches(segmentGroup, slicedSegments, config)) {
        /** @type {?} */
        const s = new UrlSegmentGroup(segmentGroup.segments, addEmptyPathsToChildrenIfNeeded(segmentGroup, consumedSegments, slicedSegments, config, segmentGroup.children, relativeLinkResolution));
        s._sourceSegment = segmentGroup;
        s._segmentIndexShift = consumedSegments.length;
        return { segmentGroup: s, slicedSegments };
    }
    /** @type {?} */
    const s = new UrlSegmentGroup(segmentGroup.segments, segmentGroup.children);
    s._sourceSegment = segmentGroup;
    s._segmentIndexShift = consumedSegments.length;
    return { segmentGroup: s, slicedSegments };
}
/**
 * @param {?} segmentGroup
 * @param {?} consumedSegments
 * @param {?} slicedSegments
 * @param {?} routes
 * @param {?} children
 * @param {?} relativeLinkResolution
 * @return {?}
 */
function addEmptyPathsToChildrenIfNeeded(segmentGroup, consumedSegments, slicedSegments, routes, children, relativeLinkResolution) {
    /** @type {?} */
    const res = {};
    for (const r of routes) {
        if (emptyPathMatch(segmentGroup, slicedSegments, r) && !children[getOutlet$1(r)]) {
            /** @type {?} */
            const s = new UrlSegmentGroup([], {});
            s._sourceSegment = segmentGroup;
            if (relativeLinkResolution === 'legacy') {
                s._segmentIndexShift = segmentGroup.segments.length;
            }
            else {
                s._segmentIndexShift = consumedSegments.length;
            }
            res[getOutlet$1(r)] = s;
        }
    }
    return Object.assign({}, children, res);
}
/**
 * @param {?} segmentGroup
 * @param {?} consumedSegments
 * @param {?} routes
 * @param {?} primarySegment
 * @return {?}
 */
function createChildrenForEmptyPaths(segmentGroup, consumedSegments, routes, primarySegment) {
    /** @type {?} */
    const res = {};
    res[PRIMARY_OUTLET] = primarySegment;
    primarySegment._sourceSegment = segmentGroup;
    primarySegment._segmentIndexShift = consumedSegments.length;
    for (const r of routes) {
        if (r.path === '' && getOutlet$1(r) !== PRIMARY_OUTLET) {
            /** @type {?} */
            const s = new UrlSegmentGroup([], {});
            s._sourceSegment = segmentGroup;
            s._segmentIndexShift = consumedSegments.length;
            res[getOutlet$1(r)] = s;
        }
    }
    return res;
}
/**
 * @param {?} segmentGroup
 * @param {?} slicedSegments
 * @param {?} routes
 * @return {?}
 */
function containsEmptyPathMatchesWithNamedOutlets(segmentGroup, slicedSegments, routes) {
    return routes.some((/**
     * @param {?} r
     * @return {?}
     */
    r => emptyPathMatch(segmentGroup, slicedSegments, r) && getOutlet$1(r) !== PRIMARY_OUTLET));
}
/**
 * @param {?} segmentGroup
 * @param {?} slicedSegments
 * @param {?} routes
 * @return {?}
 */
function containsEmptyPathMatches(segmentGroup, slicedSegments, routes) {
    return routes.some((/**
     * @param {?} r
     * @return {?}
     */
    r => emptyPathMatch(segmentGroup, slicedSegments, r)));
}
/**
 * @param {?} segmentGroup
 * @param {?} slicedSegments
 * @param {?} r
 * @return {?}
 */
function emptyPathMatch(segmentGroup, slicedSegments, r) {
    if ((segmentGroup.hasChildren() || slicedSegments.length > 0) && r.pathMatch === 'full') {
        return false;
    }
    return r.path === '' && r.redirectTo === undefined;
}
/**
 * @param {?} route
 * @return {?}
 */
function getOutlet$1(route) {
    return route.outlet || PRIMARY_OUTLET;
}
/**
 * @param {?} route
 * @return {?}
 */
function getData(route) {
    return route.data || {};
}
/**
 * @param {?} route
 * @return {?}
 */
function getResolve(route) {
    return route.resolve || {};
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} rootComponentType
 * @param {?} config
 * @param {?} serializer
 * @param {?} paramsInheritanceStrategy
 * @param {?} relativeLinkResolution
 * @return {?}
 */
function recognize$1(rootComponentType, config, serializer, paramsInheritanceStrategy, relativeLinkResolution) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => recognize(rootComponentType, config, t.urlAfterRedirects, serializer(t.urlAfterRedirects), paramsInheritanceStrategy, relativeLinkResolution)
            .pipe(map((/**
         * @param {?} targetSnapshot
         * @return {?}
         */
        targetSnapshot => (Object.assign({}, t, { targetSnapshot }))))))));
    });
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} paramsInheritanceStrategy
 * @param {?} moduleInjector
 * @return {?}
 */
function resolveData(paramsInheritanceStrategy, moduleInjector) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            const { targetSnapshot, guards: { canActivateChecks } } = t;
            if (!canActivateChecks.length) {
                return of(t);
            }
            return from(canActivateChecks)
                .pipe(concatMap((/**
             * @param {?} check
             * @return {?}
             */
            check => runResolve(check.route, (/** @type {?} */ (targetSnapshot)), paramsInheritanceStrategy, moduleInjector))), reduce((/**
             * @param {?} _
             * @param {?} __
             * @return {?}
             */
            (_, __) => _)), map((/**
             * @param {?} _
             * @return {?}
             */
            _ => t)));
        })));
    });
}
/**
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} paramsInheritanceStrategy
 * @param {?} moduleInjector
 * @return {?}
 */
function runResolve(futureARS, futureRSS, paramsInheritanceStrategy, moduleInjector) {
    /** @type {?} */
    const resolve = futureARS._resolve;
    return resolveNode(resolve, futureARS, futureRSS, moduleInjector)
        .pipe(map((/**
     * @param {?} resolvedData
     * @return {?}
     */
    (resolvedData) => {
        futureARS._resolvedData = resolvedData;
        futureARS.data = Object.assign({}, futureARS.data, inheritedParamsDataResolve(futureARS, paramsInheritanceStrategy).resolve);
        return null;
    })));
}
/**
 * @param {?} resolve
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function resolveNode(resolve, futureARS, futureRSS, moduleInjector) {
    /** @type {?} */
    const keys = Object.keys(resolve);
    if (keys.length === 0) {
        return of({});
    }
    if (keys.length === 1) {
        /** @type {?} */
        const key = keys[0];
        return getResolver(resolve[key], futureARS, futureRSS, moduleInjector)
            .pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        (value) => { return { [key]: value }; })));
    }
    /** @type {?} */
    const data = {};
    /** @type {?} */
    const runningResolvers$ = from(keys).pipe(mergeMap((/**
     * @param {?} key
     * @return {?}
     */
    (key) => {
        return getResolver(resolve[key], futureARS, futureRSS, moduleInjector)
            .pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        (value) => {
            data[key] = value;
            return value;
        })));
    })));
    return runningResolvers$.pipe(last$1(), map((/**
     * @return {?}
     */
    () => data)));
}
/**
 * @param {?} injectionToken
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function getResolver(injectionToken, futureARS, futureRSS, moduleInjector) {
    /** @type {?} */
    const resolver = getToken(injectionToken, futureARS, moduleInjector);
    return resolver.resolve ? wrapIntoObservable(resolver.resolve(futureARS, futureRSS)) :
        wrapIntoObservable(resolver(futureARS, futureRSS));
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Perform a side effect through a switchMap for every emission on the source Observable,
 * but return an Observable that is identical to the source. It's essentially the same as
 * the `tap` operator, but if the side effectful `next` function returns an ObservableInput,
 * it will wait before continuing with the original value.
 * @template T
 * @param {?} next
 * @return {?}
 */
function switchTap(next) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(switchMap((/**
         * @param {?} v
         * @return {?}
         */
        v => {
            /** @type {?} */
            const nextResult = next(v);
            if (nextResult) {
                return from(nextResult).pipe(map((/**
                 * @return {?}
                 */
                () => v)));
            }
            return from([v]);
        })));
    });
}

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
/**
 * \@description
 *
 * Provides a way to customize when activated routes get reused.
 *
 * \@publicApi
 * @abstract
 */
class RouteReuseStrategy {
}
if (false) {
    /**
     * Determines if this route (and its subtree) should be detached to be reused later
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldDetach = function (route) { };
    /**
     * Stores the detached route.
     *
     * Storing a `null` value should erase the previously stored value.
     * @abstract
     * @param {?} route
     * @param {?} handle
     * @return {?}
     */
    RouteReuseStrategy.prototype.store = function (route, handle) { };
    /**
     * Determines if this route (and its subtree) should be reattached
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldAttach = function (route) { };
    /**
     * Retrieves the previously stored route
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.retrieve = function (route) { };
    /**
     * Determines if a route should be reused
     * @abstract
     * @param {?} future
     * @param {?} curr
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldReuseRoute = function (future, curr) { };
}
/**
 * Does not detach any subtrees. Reuses routes as long as their route config is the same.
 */
class DefaultRouteReuseStrategy {
    /**
     * @param {?} route
     * @return {?}
     */
    shouldDetach(route) { return false; }
    /**
     * @param {?} route
     * @param {?} detachedTree
     * @return {?}
     */
    store(route, detachedTree) { }
    /**
     * @param {?} route
     * @return {?}
     */
    shouldAttach(route) { return false; }
    /**
     * @param {?} route
     * @return {?}
     */
    retrieve(route) { return null; }
    /**
     * @param {?} future
     * @param {?} curr
     * @return {?}
     */
    shouldReuseRoute(future, curr) {
        return future.routeConfig === curr.routeConfig;
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * The [DI token](guide/glossary/#di-token) for a router configuration.
 * @see `ROUTES`
 * \@publicApi
 * @type {?}
 */
const ROUTES = new InjectionToken('ROUTES');
class RouterConfigLoader {
    /**
     * @param {?} loader
     * @param {?} compiler
     * @param {?=} onLoadStartListener
     * @param {?=} onLoadEndListener
     */
    constructor(loader, compiler, onLoadStartListener, onLoadEndListener) {
        this.loader = loader;
        this.compiler = compiler;
        this.onLoadStartListener = onLoadStartListener;
        this.onLoadEndListener = onLoadEndListener;
    }
    /**
     * @param {?} parentInjector
     * @param {?} route
     * @return {?}
     */
    load(parentInjector, route) {
        if (this.onLoadStartListener) {
            this.onLoadStartListener(route);
        }
        /** @type {?} */
        const moduleFactory$ = this.loadModuleFactory((/** @type {?} */ (route.loadChildren)));
        return moduleFactory$.pipe(map((/**
         * @param {?} factory
         * @return {?}
         */
        (factory) => {
            if (this.onLoadEndListener) {
                this.onLoadEndListener(route);
            }
            /** @type {?} */
            const module = factory.create(parentInjector);
            return new LoadedRouterConfig(flatten(module.injector.get(ROUTES)).map(standardizeConfig), module);
        })));
    }
    /**
     * @private
     * @param {?} loadChildren
     * @return {?}
     */
    loadModuleFactory(loadChildren) {
        if (typeof loadChildren === 'string') {
            return from(this.loader.load(loadChildren));
        }
        else {
            return wrapIntoObservable(loadChildren()).pipe(mergeMap((/**
             * @param {?} t
             * @return {?}
             */
            (t) => {
                if (t instanceof NgModuleFactory) {
                    return of(t);
                }
                else {
                    return from(this.compiler.compileModuleAsync(t));
                }
            })));
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.loader;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.compiler;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.onLoadStartListener;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.onLoadEndListener;
}

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
/**
 * \@description
 *
 * Provides a way to migrate AngularJS applications to Angular.
 *
 * \@publicApi
 * @abstract
 */
class UrlHandlingStrategy {
}
if (false) {
    /**
     * Tells the router if this URL should be processed.
     *
     * When it returns true, the router will execute the regular navigation.
     * When it returns false, the router will set the router state to an empty state.
     * As a result, all the active components will be destroyed.
     *
     * @abstract
     * @param {?} url
     * @return {?}
     */
    UrlHandlingStrategy.prototype.shouldProcessUrl = function (url) { };
    /**
     * Extracts the part of the URL that should be handled by the router.
     * The rest of the URL will remain untouched.
     * @abstract
     * @param {?} url
     * @return {?}
     */
    UrlHandlingStrategy.prototype.extract = function (url) { };
    /**
     * Merges the URL fragment with the rest of the URL.
     * @abstract
     * @param {?} newUrlPart
     * @param {?} rawUrl
     * @return {?}
     */
    UrlHandlingStrategy.prototype.merge = function (newUrlPart, rawUrl) { };
}
/**
 * \@publicApi
 */
class DefaultUrlHandlingStrategy {
    /**
     * @param {?} url
     * @return {?}
     */
    shouldProcessUrl(url) { return true; }
    /**
     * @param {?} url
     * @return {?}
     */
    extract(url) { return url; }
    /**
     * @param {?} newUrlPart
     * @param {?} wholeUrl
     * @return {?}
     */
    merge(newUrlPart, wholeUrl) { return newUrlPart; }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * \@description
 *
 * Options that modify the navigation strategy.
 *
 * \@publicApi
 * @record
 */
function NavigationExtras() { }
if (false) {
    /**
     * Specifies a root URI to use for relative navigation.
     *
     * For example, consider the following route configuration where the parent route
     * has two children.
     *
     * ```
     * [{
     *   path: 'parent',
     *   component: ParentComponent,
     *   children: [{
     *     path: 'list',
     *     component: ListComponent
     *   },{
     *     path: 'child',
     *     component: ChildComponent
     *   }]
     * }]
     * ```
     *
     * The following `go()` function navigates to the `list` route by
     * interpreting the destination URI as relative to the activated `child`  route
     *
     * ```
     * \@Component({...})
     *  class ChildComponent {
     *    constructor(private router: Router, private route: ActivatedRoute) {}
     *
     *    go() {
     *      this.router.navigate(['../list'], { relativeTo: this.route });
     *    }
     *  }
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.relativeTo;
    /**
     * Sets query parameters to the URL.
     *
     * ```
     * // Navigate to /results?page=1
     * this.router.navigate(['/results'], { queryParams: { page: 1 } });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.queryParams;
    /**
     * Sets the hash fragment for the URL.
     *
     * ```
     * // Navigate to /results#top
     * this.router.navigate(['/results'], { fragment: 'top' });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.fragment;
    /**
     * **DEPRECATED**: Use `queryParamsHandling: "preserve"` instead to preserve
     * query parameters for the next navigation.
     *
     * @deprecated since v4
     * @type {?|undefined}
     */
    NavigationExtras.prototype.preserveQueryParams;
    /**
     * How to handle query parameters in the router link for the next navigation.
     * One of:
     * * `merge` : Merge new with current parameters.
     * * `preserve` : Preserve current parameters.
     *
     * ```
     * // from /results?page=1 to /view?page=1&page=2
     * this.router.navigate(['/view'], { queryParams: { page: 2 },  queryParamsHandling: "merge" });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.queryParamsHandling;
    /**
     * When true, preserves the URL fragment for the next navigation
     *
     * ```
     * // Preserve fragment from /results#top to /view#top
     * this.router.navigate(['/view'], { preserveFragment: true });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.preserveFragment;
    /**
     * When true, navigates without pushing a new state into history.
     *
     * ```
     * // Navigate silently to /view
     * this.router.navigate(['/view'], { skipLocationChange: true });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.skipLocationChange;
    /**
     * When true, navigates while replacing the current state in history.
     *
     * ```
     * // Navigate to /view
     * this.router.navigate(['/view'], { replaceUrl: true });
     * ```
     * @type {?|undefined}
     */
    NavigationExtras.prototype.replaceUrl;
    /**
     * Developer-defined state that can be passed to any navigation.
     * Access this value through the `Navigation.extras` object
     * returned from `router.getCurrentNavigation()` while a navigation is executing.
     *
     * After a navigation completes, the router writes an object containing this
     * value together with a `navigationId` to `history.state`.
     * The value is written when `location.go()` or `location.replaceState()`
     * is called before activating this route.
     *
     * Note that `history.state` does not pass an object equality test because
     * the router adds the `navigationId` on each navigation.
     * @type {?|undefined}
     */
    NavigationExtras.prototype.state;
}
/**
 * @param {?} error
 * @return {?}
 */
function defaultErrorHandler(error) {
    throw error;
}
/**
 * @param {?} error
 * @param {?} urlSerializer
 * @param {?} url
 * @return {?}
 */
function defaultMalformedUriErrorHandler(error, urlSerializer, url) {
    return urlSerializer.parse('/');
}
/**
 * \@internal
 * @param {?} snapshot
 * @param {?} runExtras
 * @return {?}
 */
function defaultRouterHook(snapshot, runExtras) {
    return (/** @type {?} */ (of(null)));
}
/**
 * \@description
 *
 * A service that provides navigation and URL manipulation capabilities.
 *
 * @see `Route`.
 * @see [Routing and Navigation Guide](guide/router).
 *
 * \@ngModule RouterModule
 *
 * \@publicApi
 */
class Router {
    /**
     * Creates the router service.
     * @param {?} rootComponentType
     * @param {?} urlSerializer
     * @param {?} rootContexts
     * @param {?} location
     * @param {?} injector
     * @param {?} loader
     * @param {?} compiler
     * @param {?} config
     */
    // TODO: vsavkin make internal after the final is out.
    constructor(rootComponentType, urlSerializer, rootContexts, location, injector, loader, compiler, config) {
        this.rootComponentType = rootComponentType;
        this.urlSerializer = urlSerializer;
        this.rootContexts = rootContexts;
        this.location = location;
        this.config = config;
        this.lastSuccessfulNavigation = null;
        this.currentNavigation = null;
        this.navigationId = 0;
        this.isNgZoneEnabled = false;
        /**
         * An event stream for routing events in this NgModule.
         */
        this.events = new Subject();
        /**
         * A handler for navigation errors in this NgModule.
         */
        this.errorHandler = defaultErrorHandler;
        /**
         * A handler for errors thrown by `Router.parseUrl(url)`
         * when `url` contains an invalid character.
         * The most common case is a `%` sign
         * that's not encoded and is not part of a percent encoded sequence.
         */
        this.malformedUriErrorHandler = defaultMalformedUriErrorHandler;
        /**
         * True if at least one navigation event has occurred,
         * false otherwise.
         */
        this.navigated = false;
        this.lastSuccessfulId = -1;
        /**
         * Hooks that enable you to pause navigation,
         * either before or after the preactivation phase.
         * Used by `RouterModule`.
         *
         * \@internal
         */
        this.hooks = {
            beforePreactivation: defaultRouterHook,
            afterPreactivation: defaultRouterHook
        };
        /**
         * A strategy for extracting and merging URLs.
         * Used for AngularJS to Angular migrations.
         */
        this.urlHandlingStrategy = new DefaultUrlHandlingStrategy();
        /**
         * A strategy for re-using routes.
         */
        this.routeReuseStrategy = new DefaultRouteReuseStrategy();
        /**
         * How to handle a navigation request to the current URL. One of:
         * - `'ignore'` :  The router ignores the request.
         * - `'reload'` : The router reloads the URL. Use to implement a "refresh" feature.
         */
        this.onSameUrlNavigation = 'ignore';
        /**
         * How to merge parameters, data, and resolved data from parent to child
         * routes. One of:
         *
         * - `'emptyOnly'` : Inherit parent parameters, data, and resolved data
         * for path-less or component-less routes.
         * - `'always'` : Inherit parent parameters, data, and resolved data
         * for all child routes.
         */
        this.paramsInheritanceStrategy = 'emptyOnly';
        /**
         * Determines when the router updates the browser URL.
         * By default (`"deferred"`), updates the browser URL after navigation has finished.
         * Set to `'eager'` to update the browser URL at the beginning of navigation.
         * You can choose to update early so that, if navigation fails,
         * you can show an error message with the URL that failed.
         */
        this.urlUpdateStrategy = 'deferred';
        /**
         * Enables a bug fix that corrects relative link resolution in components with empty paths.
         * @see `RouterModule`
         */
        this.relativeLinkResolution = 'legacy';
        /** @type {?} */
        const onLoadStart = (/**
         * @param {?} r
         * @return {?}
         */
        (r) => this.triggerEvent(new RouteConfigLoadStart(r)));
        /** @type {?} */
        const onLoadEnd = (/**
         * @param {?} r
         * @return {?}
         */
        (r) => this.triggerEvent(new RouteConfigLoadEnd(r)));
        this.ngModule = injector.get(NgModuleRef);
        this.console = injector.get(ɵConsole);
        /** @type {?} */
        const ngZone = injector.get(NgZone);
        this.isNgZoneEnabled = ngZone instanceof NgZone;
        this.resetConfig(config);
        this.currentUrlTree = createEmptyUrlTree();
        this.rawUrlTree = this.currentUrlTree;
        this.browserUrlTree = this.currentUrlTree;
        this.configLoader = new RouterConfigLoader(loader, compiler, onLoadStart, onLoadEnd);
        this.routerState = createEmptyState(this.currentUrlTree, this.rootComponentType);
        this.transitions = new BehaviorSubject({
            id: 0,
            currentUrlTree: this.currentUrlTree,
            currentRawUrl: this.currentUrlTree,
            extractedUrl: this.urlHandlingStrategy.extract(this.currentUrlTree),
            urlAfterRedirects: this.urlHandlingStrategy.extract(this.currentUrlTree),
            rawUrl: this.currentUrlTree,
            extras: {},
            resolve: null,
            reject: null,
            promise: Promise.resolve(true),
            source: 'imperative',
            restoredState: null,
            currentSnapshot: this.routerState.snapshot,
            targetSnapshot: null,
            currentRouterState: this.routerState,
            targetRouterState: null,
            guards: { canActivateChecks: [], canDeactivateChecks: [] },
            guardsResult: null,
        });
        this.navigations = this.setupNavigations(this.transitions);
        this.processNavigations();
    }
    /**
     * @private
     * @param {?} transitions
     * @return {?}
     */
    setupNavigations(transitions) {
        /** @type {?} */
        const eventsSubject = ((/** @type {?} */ (this.events)));
        return (/** @type {?} */ ((/** @type {?} */ (transitions.pipe(filter((/**
         * @param {?} t
         * @return {?}
         */
        t => t.id !== 0)), 
        // Extract URL
        map((/**
         * @param {?} t
         * @return {?}
         */
        t => ((/** @type {?} */ (Object.assign({}, t, { extractedUrl: this.urlHandlingStrategy.extract(t.rawUrl) })))))), 
        // Using switchMap so we cancel executing navigations when a new one comes in
        switchMap((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            /** @type {?} */
            let completed = false;
            /** @type {?} */
            let errored = false;
            return of(t).pipe(
            // Store the Navigation object
            tap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                this.currentNavigation = {
                    id: t.id,
                    initialUrl: t.currentRawUrl,
                    extractedUrl: t.extractedUrl,
                    trigger: t.source,
                    extras: t.extras,
                    previousNavigation: this.lastSuccessfulNavigation ? Object.assign({}, this.lastSuccessfulNavigation, { previousNavigation: null }) :
                        null
                };
            })), switchMap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                /** @type {?} */
                const urlTransition = !this.navigated || t.extractedUrl.toString() !== this.browserUrlTree.toString();
                /** @type {?} */
                const processCurrentUrl = (this.onSameUrlNavigation === 'reload' ? true : urlTransition) &&
                    this.urlHandlingStrategy.shouldProcessUrl(t.rawUrl);
                if (processCurrentUrl) {
                    return of(t).pipe(
                    // Fire NavigationStart event
                    switchMap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        /** @type {?} */
                        const transition = this.transitions.getValue();
                        eventsSubject.next(new NavigationStart(t.id, this.serializeUrl(t.extractedUrl), t.source, t.restoredState));
                        if (transition !== this.transitions.getValue()) {
                            return EMPTY;
                        }
                        return [t];
                    })), 
                    // This delay is required to match old behavior that forced navigation to
                    // always be async
                    switchMap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => Promise.resolve(t))), 
                    // ApplyRedirects
                    applyRedirects$1(this.ngModule.injector, this.configLoader, this.urlSerializer, this.config), 
                    // Update the currentNavigation
                    tap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        this.currentNavigation = Object.assign({}, (/** @type {?} */ (this.currentNavigation)), { finalUrl: t.urlAfterRedirects });
                    })), 
                    // Recognize
                    recognize$1(this.rootComponentType, this.config, (/**
                     * @param {?} url
                     * @return {?}
                     */
                    (url) => this.serializeUrl(url)), this.paramsInheritanceStrategy, this.relativeLinkResolution), 
                    // Update URL if in `eager` update mode
                    tap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        if (this.urlUpdateStrategy === 'eager') {
                            if (!t.extras.skipLocationChange) {
                                this.setBrowserUrl(t.urlAfterRedirects, !!t.extras.replaceUrl, t.id, t.extras.state);
                            }
                            this.browserUrlTree = t.urlAfterRedirects;
                        }
                    })), 
                    // Fire RoutesRecognized
                    tap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        /** @type {?} */
                        const routesRecognized = new RoutesRecognized(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(t.urlAfterRedirects), (/** @type {?} */ (t.targetSnapshot)));
                        eventsSubject.next(routesRecognized);
                    })));
                }
                else {
                    /** @type {?} */
                    const processPreviousUrl = urlTransition && this.rawUrlTree &&
                        this.urlHandlingStrategy.shouldProcessUrl(this.rawUrlTree);
                    /* When the current URL shouldn't be processed, but the previous one was, we
                     * handle this "error condition" by navigating to the previously successful URL,
                     * but leaving the URL intact.*/
                    if (processPreviousUrl) {
                        const { id, extractedUrl, source, restoredState, extras } = t;
                        /** @type {?} */
                        const navStart = new NavigationStart(id, this.serializeUrl(extractedUrl), source, restoredState);
                        eventsSubject.next(navStart);
                        /** @type {?} */
                        const targetSnapshot = createEmptyState(extractedUrl, this.rootComponentType).snapshot;
                        return of(Object.assign({}, t, { targetSnapshot, urlAfterRedirects: extractedUrl, extras: Object.assign({}, extras, { skipLocationChange: false, replaceUrl: false }) }));
                    }
                    else {
                        /* When neither the current or previous URL can be processed, do nothing other
                         * than update router's internal reference to the current "settled" URL. This
                         * way the next navigation will be coming from the current URL in the browser.
                         */
                        this.rawUrlTree = t.rawUrl;
                        this.browserUrlTree = t.urlAfterRedirects;
                        t.resolve(null);
                        return EMPTY;
                    }
                }
            })), 
            // Before Preactivation
            switchTap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                const { targetSnapshot, id: navigationId, extractedUrl: appliedUrlTree, rawUrl: rawUrlTree, extras: { skipLocationChange, replaceUrl } } = t;
                return this.hooks.beforePreactivation((/** @type {?} */ (targetSnapshot)), {
                    navigationId,
                    appliedUrlTree,
                    rawUrlTree,
                    skipLocationChange: !!skipLocationChange,
                    replaceUrl: !!replaceUrl,
                });
            })), 
            // --- GUARDS ---
            tap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                /** @type {?} */
                const guardsStart = new GuardsCheckStart(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(t.urlAfterRedirects), (/** @type {?} */ (t.targetSnapshot)));
                this.triggerEvent(guardsStart);
            })), map((/**
             * @param {?} t
             * @return {?}
             */
            t => (Object.assign({}, t, { guards: getAllRouteGuards((/** @type {?} */ (t.targetSnapshot)), t.currentSnapshot, this.rootContexts) })))), checkGuards(this.ngModule.injector, (/**
             * @param {?} evt
             * @return {?}
             */
            (evt) => this.triggerEvent(evt))), tap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                if (isUrlTree(t.guardsResult)) {
                    /** @type {?} */
                    const error = navigationCancelingError(`Redirecting to "${this.serializeUrl(t.guardsResult)}"`);
                    error.url = t.guardsResult;
                    throw error;
                }
            })), tap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                /** @type {?} */
                const guardsEnd = new GuardsCheckEnd(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(t.urlAfterRedirects), (/** @type {?} */ (t.targetSnapshot)), !!t.guardsResult);
                this.triggerEvent(guardsEnd);
            })), filter((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                if (!t.guardsResult) {
                    this.resetUrlToCurrentUrlTree();
                    /** @type {?} */
                    const navCancel = new NavigationCancel(t.id, this.serializeUrl(t.extractedUrl), '');
                    eventsSubject.next(navCancel);
                    t.resolve(false);
                    return false;
                }
                return true;
            })), 
            // --- RESOLVE ---
            switchTap((/**
             * @param {?} t
             * @return {?}
             */
            t => {
                if (t.guards.canActivateChecks.length) {
                    return of(t).pipe(tap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        /** @type {?} */
                        const resolveStart = new ResolveStart(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(t.urlAfterRedirects), (/** @type {?} */ (t.targetSnapshot)));
                        this.triggerEvent(resolveStart);
                    })), resolveData(this.paramsInheritanceStrategy, this.ngModule.injector), //
                    tap((/**
                     * @param {?} t
                     * @return {?}
                     */
                    t => {
                        /** @type {?} */
                        const resolveEnd = new ResolveEnd(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(t.urlAfterRedirects), (/** @type {?} */ (t.targetSnapshot)));
                        this.triggerEvent(resolveEnd);
                    })));
                }
                return undefined;
            })), 
            // --- AFTER PREACTIVATION ---
            switchTap((/**
             * @param {?} t
             * @return {?}
             */
            (t) => {
                const { targetSnapshot, id: navigationId, extractedUrl: appliedUrlTree, rawUrl: rawUrlTree, extras: { skipLocationChange, replaceUrl } } = t;
                return this.hooks.afterPreactivation((/** @type {?} */ (targetSnapshot)), {
                    navigationId,
                    appliedUrlTree,
                    rawUrlTree,
                    skipLocationChange: !!skipLocationChange,
                    replaceUrl: !!replaceUrl,
                });
            })), map((/**
             * @param {?} t
             * @return {?}
             */
            (t) => {
                /** @type {?} */
                const targetRouterState = createRouterState(this.routeReuseStrategy, (/** @type {?} */ (t.targetSnapshot)), t.currentRouterState);
                return (Object.assign({}, t, { targetRouterState }));
            })), 
            /* Once here, we are about to activate syncronously. The assumption is this will
               succeed, and user code may read from the Router service. Therefore before
               activation, we need to update router properties storing the current URL and the
               RouterState, as well as updated the browser URL. All this should happen *before*
               activating. */
            tap((/**
             * @param {?} t
             * @return {?}
             */
            (t) => {
                this.currentUrlTree = t.urlAfterRedirects;
                this.rawUrlTree = this.urlHandlingStrategy.merge(this.currentUrlTree, t.rawUrl);
                ((/** @type {?} */ (this))).routerState = (/** @type {?} */ (t.targetRouterState));
                if (this.urlUpdateStrategy === 'deferred') {
                    if (!t.extras.skipLocationChange) {
                        this.setBrowserUrl(this.rawUrlTree, !!t.extras.replaceUrl, t.id, t.extras.state);
                    }
                    this.browserUrlTree = t.urlAfterRedirects;
                }
            })), activateRoutes(this.rootContexts, this.routeReuseStrategy, (/**
             * @param {?} evt
             * @return {?}
             */
            (evt) => this.triggerEvent(evt))), tap({ /**
                 * @return {?}
                 */
                next() { completed = true; }, /**
                 * @return {?}
                 */
                complete() { completed = true; } }), finalize((/**
             * @return {?}
             */
            () => {
                /* When the navigation stream finishes either through error or success, we set the
                 * `completed` or `errored` flag. However, there are some situations where we could
                 * get here without either of those being set. For instance, a redirect during
                 * NavigationStart. Therefore, this is a catch-all to make sure the NavigationCancel
                 * event is fired when a navigation gets cancelled but not caught by other means. */
                if (!completed && !errored) {
                    // Must reset to current URL tree here to ensure history.state is set. On a fresh
                    // page load, if a new navigation comes in before a successful navigation
                    // completes, there will be nothing in history.state.navigationId. This can cause
                    // sync problems with AngularJS sync code which looks for a value here in order
                    // to determine whether or not to handle a given popstate event or to leave it
                    // to the Angualr router.
                    this.resetUrlToCurrentUrlTree();
                    /** @type {?} */
                    const navCancel = new NavigationCancel(t.id, this.serializeUrl(t.extractedUrl), `Navigation ID ${t.id} is not equal to the current navigation id ${this.navigationId}`);
                    eventsSubject.next(navCancel);
                    t.resolve(false);
                }
                // currentNavigation should always be reset to null here. If navigation was
                // successful, lastSuccessfulTransition will have already been set. Therefore we
                // can safely set currentNavigation to null here.
                this.currentNavigation = null;
            })), catchError((/**
             * @param {?} e
             * @return {?}
             */
            (e) => {
                errored = true;
                /* This error type is issued during Redirect, and is handled as a cancellation
                 * rather than an error. */
                if (isNavigationCancelingError(e)) {
                    /** @type {?} */
                    const redirecting = isUrlTree(e.url);
                    if (!redirecting) {
                        // Set property only if we're not redirecting. If we landed on a page and
                        // redirect to `/` route, the new navigation is going to see the `/` isn't
                        // a change from the default currentUrlTree and won't navigate. This is
                        // only applicable with initial navigation, so setting `navigated` only when
                        // not redirecting resolves this scenario.
                        this.navigated = true;
                        this.resetStateAndUrl(t.currentRouterState, t.currentUrlTree, t.rawUrl);
                    }
                    /** @type {?} */
                    const navCancel = new NavigationCancel(t.id, this.serializeUrl(t.extractedUrl), e.message);
                    eventsSubject.next(navCancel);
                    t.resolve(false);
                    if (redirecting) {
                        this.navigateByUrl(e.url);
                    }
                    /* All other errors should reset to the router's internal URL reference to the
                     * pre-error state. */
                }
                else {
                    this.resetStateAndUrl(t.currentRouterState, t.currentUrlTree, t.rawUrl);
                    /** @type {?} */
                    const navError = new NavigationError(t.id, this.serializeUrl(t.extractedUrl), e);
                    eventsSubject.next(navError);
                    try {
                        t.resolve(this.errorHandler(e));
                    }
                    catch (ee) {
                        t.reject(ee);
                    }
                }
                return EMPTY;
            })));
            // TODO(jasonaden): remove cast once g3 is on updated TypeScript
        })))))));
    }
    /**
     * \@internal
     * TODO: this should be removed once the constructor of the router made internal
     * @param {?} rootComponentType
     * @return {?}
     */
    resetRootComponentType(rootComponentType) {
        this.rootComponentType = rootComponentType;
        // TODO: vsavkin router 4.0 should make the root component set to null
        // this will simplify the lifecycle of the router.
        this.routerState.root.component = this.rootComponentType;
    }
    /**
     * @private
     * @return {?}
     */
    getTransition() {
        /** @type {?} */
        const transition = this.transitions.value;
        // This value needs to be set. Other values such as extractedUrl are set on initial navigation
        // but the urlAfterRedirects may not get set if we aren't processing the new URL *and* not
        // processing the previous URL.
        transition.urlAfterRedirects = this.browserUrlTree;
        return transition;
    }
    /**
     * @private
     * @param {?} t
     * @return {?}
     */
    setTransition(t) {
        this.transitions.next(Object.assign({}, this.getTransition(), t));
    }
    /**
     * Sets up the location change listener and performs the initial navigation.
     * @return {?}
     */
    initialNavigation() {
        this.setUpLocationChangeListener();
        if (this.navigationId === 0) {
            this.navigateByUrl(this.location.path(true), { replaceUrl: true });
        }
    }
    /**
     * Sets up the location change listener.
     * @return {?}
     */
    setUpLocationChangeListener() {
        // Don't need to use Zone.wrap any more, because zone.js
        // already patch onPopState, so location change callback will
        // run into ngZone
        if (!this.locationSubscription) {
            this.locationSubscription = (/** @type {?} */ (this.location.subscribe((/**
             * @param {?} change
             * @return {?}
             */
            (change) => {
                /** @type {?} */
                let rawUrlTree = this.parseUrl(change['url']);
                /** @type {?} */
                const source = change['type'] === 'popstate' ? 'popstate' : 'hashchange';
                // Navigations coming from Angular router have a navigationId state property. When this
                // exists, restore the state.
                /** @type {?} */
                const state = change.state && change.state.navigationId ? change.state : null;
                setTimeout((/**
                 * @return {?}
                 */
                () => { this.scheduleNavigation(rawUrlTree, source, state, { replaceUrl: true }); }), 0);
            }))));
        }
    }
    /**
     * The current URL.
     * @return {?}
     */
    get url() { return this.serializeUrl(this.currentUrlTree); }
    /**
     * The current Navigation object if one exists
     * @return {?}
     */
    getCurrentNavigation() { return this.currentNavigation; }
    /**
     * \@internal
     * @param {?} event
     * @return {?}
     */
    triggerEvent(event) { ((/** @type {?} */ (this.events))).next(event); }
    /**
     * Resets the configuration used for navigation and generating links.
     *
     * \@usageNotes
     *
     * ```
     * router.resetConfig([
     *  { path: 'team/:id', component: TeamCmp, children: [
     *    { path: 'simple', component: SimpleCmp },
     *    { path: 'user/:name', component: UserCmp }
     *  ]}
     * ]);
     * ```
     * @param {?} config The route array for the new configuration.
     *
     * @return {?}
     */
    resetConfig(config) {
        validateConfig(config);
        this.config = config.map(standardizeConfig);
        this.navigated = false;
        this.lastSuccessfulId = -1;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    ngOnDestroy() { this.dispose(); }
    /**
     * Disposes of the router.
     * @return {?}
     */
    dispose() {
        if (this.locationSubscription) {
            this.locationSubscription.unsubscribe();
            this.locationSubscription = (/** @type {?} */ (null));
        }
    }
    /**
     * Applies an array of commands to the current URL tree and creates a new URL tree.
     *
     * When given an activated route, applies the given commands starting from the route.
     * Otherwise, applies the given command starting from the root.
     *
     * \@usageNotes
     *
     * ```
     * // create /team/33/user/11
     * router.createUrlTree(['/team', 33, 'user', 11]);
     *
     * // create /team/33;expand=true/user/11
     * router.createUrlTree(['/team', 33, {expand: true}, 'user', 11]);
     *
     * // you can collapse static segments like this (this works only with the first passed-in value):
     * router.createUrlTree(['/team/33/user', userId]);
     *
     * // If the first segment can contain slashes, and you do not want the router to split it,
     * // you can do the following:
     * router.createUrlTree([{segmentPath: '/one/two'}]);
     *
     * // create /team/33/(user/11//right:chat)
     * router.createUrlTree(['/team', 33, {outlets: {primary: 'user/11', right: 'chat'}}]);
     *
     * // remove the right secondary node
     * router.createUrlTree(['/team', 33, {outlets: {primary: 'user/11', right: null}}]);
     *
     * // assuming the current url is `/team/33/user/11` and the route points to `user/11`
     *
     * // navigate to /team/33/user/11/details
     * router.createUrlTree(['details'], {relativeTo: route});
     *
     * // navigate to /team/33/user/22
     * router.createUrlTree(['../22'], {relativeTo: route});
     *
     * // navigate to /team/44/user/22
     * router.createUrlTree(['../../team/44/user/22'], {relativeTo: route});
     * ```
     * @param {?} commands An array of commands to apply.
     * @param {?=} navigationExtras Options that control the navigation strategy.
     * @return {?} The new URL tree.
     *
     */
    createUrlTree(commands, navigationExtras = {}) {
        const { relativeTo, queryParams, fragment, preserveQueryParams, queryParamsHandling, preserveFragment } = navigationExtras;
        if (isDevMode() && preserveQueryParams && (/** @type {?} */ (console)) && (/** @type {?} */ (console.warn))) {
            console.warn('preserveQueryParams is deprecated, use queryParamsHandling instead.');
        }
        /** @type {?} */
        const a = relativeTo || this.routerState.root;
        /** @type {?} */
        const f = preserveFragment ? this.currentUrlTree.fragment : fragment;
        /** @type {?} */
        let q = null;
        if (queryParamsHandling) {
            switch (queryParamsHandling) {
                case 'merge':
                    q = Object.assign({}, this.currentUrlTree.queryParams, queryParams);
                    break;
                case 'preserve':
                    q = this.currentUrlTree.queryParams;
                    break;
                default:
                    q = queryParams || null;
            }
        }
        else {
            q = preserveQueryParams ? this.currentUrlTree.queryParams : queryParams || null;
        }
        if (q !== null) {
            q = this.removeEmptyProps(q);
        }
        return createUrlTree(a, this.currentUrlTree, commands, (/** @type {?} */ (q)), (/** @type {?} */ (f)));
    }
    /**
     * Navigate based on the provided URL, which must be absolute.
     *
     * \@usageNotes
     *
     * ```
     * router.navigateByUrl("/team/33/user/11");
     *
     * // Navigate without updating the URL
     * router.navigateByUrl("/team/33/user/11", { skipLocationChange: true });
     * ```
     *
     * @param {?} url An absolute URL. The function does not apply any delta to the current URL.
     * @param {?=} extras An object containing properties that modify the navigation strategy.
     * The function ignores any properties in the `NavigationExtras` that would change the
     * provided URL.
     *
     * @return {?} A Promise that resolves to 'true' when navigation succeeds,
     * to 'false' when navigation fails, or is rejected on error.
     *
     */
    navigateByUrl(url, extras = { skipLocationChange: false }) {
        if (isDevMode() && this.isNgZoneEnabled && !NgZone.isInAngularZone()) {
            this.console.warn(`Navigation triggered outside Angular zone, did you forget to call 'ngZone.run()'?`);
        }
        /** @type {?} */
        const urlTree = isUrlTree(url) ? url : this.parseUrl(url);
        /** @type {?} */
        const mergedTree = this.urlHandlingStrategy.merge(urlTree, this.rawUrlTree);
        return this.scheduleNavigation(mergedTree, 'imperative', null, extras);
    }
    /**
     * Navigate based on the provided array of commands and a starting point.
     * If no starting route is provided, the navigation is absolute.
     *
     * Returns a promise that:
     * - resolves to 'true' when navigation succeeds,
     * - resolves to 'false' when navigation fails,
     * - is rejected when an error happens.
     *
     * \@usageNotes
     *
     * ```
     * router.navigate(['team', 33, 'user', 11], {relativeTo: route});
     *
     * // Navigate without updating the URL
     * router.navigate(['team', 33, 'user', 11], {relativeTo: route, skipLocationChange: true});
     * ```
     *
     * The first parameter of `navigate()` is a delta to be applied to the current URL
     * or the one provided in the `relativeTo` property of the second parameter (the
     * `NavigationExtras`).
     *
     * In order to affect this browser's `history.state` entry, the `state`
     * parameter can be passed. This must be an object because the router
     * will add the `navigationId` property to this object before creating
     * the new history item.
     * @param {?} commands
     * @param {?=} extras
     * @return {?}
     */
    navigate(commands, extras = { skipLocationChange: false }) {
        validateCommands(commands);
        return this.navigateByUrl(this.createUrlTree(commands, extras), extras);
    }
    /**
     * Serializes a `UrlTree` into a string
     * @param {?} url
     * @return {?}
     */
    serializeUrl(url) { return this.urlSerializer.serialize(url); }
    /**
     * Parses a string into a `UrlTree`
     * @param {?} url
     * @return {?}
     */
    parseUrl(url) {
        /** @type {?} */
        let urlTree;
        try {
            urlTree = this.urlSerializer.parse(url);
        }
        catch (e) {
            urlTree = this.malformedUriErrorHandler(e, this.urlSerializer, url);
        }
        return urlTree;
    }
    /**
     * Returns whether the url is activated
     * @param {?} url
     * @param {?} exact
     * @return {?}
     */
    isActive(url, exact) {
        if (isUrlTree(url)) {
            return containsTree(this.currentUrlTree, url, exact);
        }
        /** @type {?} */
        const urlTree = this.parseUrl(url);
        return containsTree(this.currentUrlTree, urlTree, exact);
    }
    /**
     * @private
     * @param {?} params
     * @return {?}
     */
    removeEmptyProps(params) {
        return Object.keys(params).reduce((/**
         * @param {?} result
         * @param {?} key
         * @return {?}
         */
        (result, key) => {
            /** @type {?} */
            const value = params[key];
            if (value !== null && value !== undefined) {
                result[key] = value;
            }
            return result;
        }), {});
    }
    /**
     * @private
     * @return {?}
     */
    processNavigations() {
        this.navigations.subscribe((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            this.navigated = true;
            this.lastSuccessfulId = t.id;
            ((/** @type {?} */ (this.events)))
                .next(new NavigationEnd(t.id, this.serializeUrl(t.extractedUrl), this.serializeUrl(this.currentUrlTree)));
            this.lastSuccessfulNavigation = this.currentNavigation;
            this.currentNavigation = null;
            t.resolve(true);
        }), (/**
         * @param {?} e
         * @return {?}
         */
        e => { this.console.warn(`Unhandled Navigation Error: `); }));
    }
    /**
     * @private
     * @param {?} rawUrl
     * @param {?} source
     * @param {?} restoredState
     * @param {?} extras
     * @return {?}
     */
    scheduleNavigation(rawUrl, source, restoredState, extras) {
        /** @type {?} */
        const lastNavigation = this.getTransition();
        // If the user triggers a navigation imperatively (e.g., by using navigateByUrl),
        // and that navigation results in 'replaceState' that leads to the same URL,
        // we should skip those.
        if (lastNavigation && source !== 'imperative' && lastNavigation.source === 'imperative' &&
            lastNavigation.rawUrl.toString() === rawUrl.toString()) {
            return Promise.resolve(true); // return value is not used
        }
        // Because of a bug in IE and Edge, the location class fires two events (popstate and
        // hashchange) every single time. The second one should be ignored. Otherwise, the URL will
        // flicker. Handles the case when a popstate was emitted first.
        if (lastNavigation && source == 'hashchange' && lastNavigation.source === 'popstate' &&
            lastNavigation.rawUrl.toString() === rawUrl.toString()) {
            return Promise.resolve(true); // return value is not used
        }
        // Because of a bug in IE and Edge, the location class fires two events (popstate and
        // hashchange) every single time. The second one should be ignored. Otherwise, the URL will
        // flicker. Handles the case when a hashchange was emitted first.
        if (lastNavigation && source == 'popstate' && lastNavigation.source === 'hashchange' &&
            lastNavigation.rawUrl.toString() === rawUrl.toString()) {
            return Promise.resolve(true); // return value is not used
        }
        /** @type {?} */
        let resolve = null;
        /** @type {?} */
        let reject = null;
        /** @type {?} */
        const promise = new Promise((/**
         * @param {?} res
         * @param {?} rej
         * @return {?}
         */
        (res, rej) => {
            resolve = res;
            reject = rej;
        }));
        /** @type {?} */
        const id = ++this.navigationId;
        this.setTransition({
            id,
            source,
            restoredState,
            currentUrlTree: this.currentUrlTree,
            currentRawUrl: this.rawUrlTree, rawUrl, extras, resolve, reject, promise,
            currentSnapshot: this.routerState.snapshot,
            currentRouterState: this.routerState
        });
        // Make sure that the error is propagated even though `processNavigations` catch
        // handler does not rethrow
        return promise.catch((/**
         * @param {?} e
         * @return {?}
         */
        (e) => { return Promise.reject(e); }));
    }
    /**
     * @private
     * @param {?} url
     * @param {?} replaceUrl
     * @param {?} id
     * @param {?=} state
     * @return {?}
     */
    setBrowserUrl(url, replaceUrl, id, state) {
        /** @type {?} */
        const path = this.urlSerializer.serialize(url);
        state = state || {};
        if (this.location.isCurrentPathEqualTo(path) || replaceUrl) {
            // TODO(jasonaden): Remove first `navigationId` and rely on `ng` namespace.
            this.location.replaceState(path, '', Object.assign({}, state, { navigationId: id }));
        }
        else {
            this.location.go(path, '', Object.assign({}, state, { navigationId: id }));
        }
    }
    /**
     * @private
     * @param {?} storedState
     * @param {?} storedUrl
     * @param {?} rawUrl
     * @return {?}
     */
    resetStateAndUrl(storedState, storedUrl, rawUrl) {
        ((/** @type {?} */ (this))).routerState = storedState;
        this.currentUrlTree = storedUrl;
        this.rawUrlTree = this.urlHandlingStrategy.merge(this.currentUrlTree, rawUrl);
        this.resetUrlToCurrentUrlTree();
    }
    /**
     * @private
     * @return {?}
     */
    resetUrlToCurrentUrlTree() {
        this.location.replaceState(this.urlSerializer.serialize(this.rawUrlTree), '', { navigationId: this.lastSuccessfulId });
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    Router.prototype.currentUrlTree;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.rawUrlTree;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.browserUrlTree;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.transitions;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.navigations;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.lastSuccessfulNavigation;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.currentNavigation;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.locationSubscription;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.navigationId;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.configLoader;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.ngModule;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.console;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.isNgZoneEnabled;
    /**
     * An event stream for routing events in this NgModule.
     * @type {?}
     */
    Router.prototype.events;
    /**
     * The current state of routing in this NgModule.
     * @type {?}
     */
    Router.prototype.routerState;
    /**
     * A handler for navigation errors in this NgModule.
     * @type {?}
     */
    Router.prototype.errorHandler;
    /**
     * A handler for errors thrown by `Router.parseUrl(url)`
     * when `url` contains an invalid character.
     * The most common case is a `%` sign
     * that's not encoded and is not part of a percent encoded sequence.
     * @type {?}
     */
    Router.prototype.malformedUriErrorHandler;
    /**
     * True if at least one navigation event has occurred,
     * false otherwise.
     * @type {?}
     */
    Router.prototype.navigated;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.lastSuccessfulId;
    /**
     * Hooks that enable you to pause navigation,
     * either before or after the preactivation phase.
     * Used by `RouterModule`.
     *
     * \@internal
     * @type {?}
     */
    Router.prototype.hooks;
    /**
     * A strategy for extracting and merging URLs.
     * Used for AngularJS to Angular migrations.
     * @type {?}
     */
    Router.prototype.urlHandlingStrategy;
    /**
     * A strategy for re-using routes.
     * @type {?}
     */
    Router.prototype.routeReuseStrategy;
    /**
     * How to handle a navigation request to the current URL. One of:
     * - `'ignore'` :  The router ignores the request.
     * - `'reload'` : The router reloads the URL. Use to implement a "refresh" feature.
     * @type {?}
     */
    Router.prototype.onSameUrlNavigation;
    /**
     * How to merge parameters, data, and resolved data from parent to child
     * routes. One of:
     *
     * - `'emptyOnly'` : Inherit parent parameters, data, and resolved data
     * for path-less or component-less routes.
     * - `'always'` : Inherit parent parameters, data, and resolved data
     * for all child routes.
     * @type {?}
     */
    Router.prototype.paramsInheritanceStrategy;
    /**
     * Determines when the router updates the browser URL.
     * By default (`"deferred"`), updates the browser URL after navigation has finished.
     * Set to `'eager'` to update the browser URL at the beginning of navigation.
     * You can choose to update early so that, if navigation fails,
     * you can show an error message with the URL that failed.
     * @type {?}
     */
    Router.prototype.urlUpdateStrategy;
    /**
     * Enables a bug fix that corrects relative link resolution in components with empty paths.
     * @see `RouterModule`
     * @type {?}
     */
    Router.prototype.relativeLinkResolution;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.rootComponentType;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.urlSerializer;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.rootContexts;
    /**
     * @type {?}
     * @private
     */
    Router.prototype.location;
    /** @type {?} */
    Router.prototype.config;
}
/**
 * @param {?} commands
 * @return {?}
 */
function validateCommands(commands) {
    for (let i = 0; i < commands.length; i++) {
        /** @type {?} */
        const cmd = commands[i];
        if (cmd == null) {
            throw new Error(`The requested path contains ${cmd} segment at index ${i}`);
        }
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * \@description
 *
 * Lets you link to specific routes in your app.
 *
 * Consider the following route configuration:
 * `[{ path: 'user/:name', component: UserCmp }]`.
 * When linking to this `user/:name` route, you use the `RouterLink` directive.
 *
 * If the link is static, you can use the directive as follows:
 * `<a routerLink="/user/bob">link to user component</a>`
 *
 * If you use dynamic values to generate the link, you can pass an array of path
 * segments, followed by the params for each segment.
 *
 * For instance `['/team', teamId, 'user', userName, {details: true}]`
 * means that we want to generate a link to `/team/11/user/bob;details=true`.
 *
 * Multiple static segments can be merged into one
 * (e.g., `['/team/11/user', userName, {details: true}]`).
 *
 * The first segment name can be prepended with `/`, `./`, or `../`:
 * * If the first segment begins with `/`, the router will look up the route from the root of the
 *   app.
 * * If the first segment begins with `./`, or doesn't begin with a slash, the router will
 *   instead look in the children of the current activated route.
 * * And if the first segment begins with `../`, the router will go up one level.
 *
 * You can set query params and fragment as follows:
 *
 * ```
 * <a [routerLink]="['/user/bob']" [queryParams]="{debug: true}" fragment="education">
 *   link to user component
 * </a>
 * ```
 * RouterLink will use these to generate this link: `/user/bob#education?debug=true`.
 *
 * (Deprecated in v4.0.0 use `queryParamsHandling` instead) You can also tell the
 * directive to preserve the current query params and fragment:
 *
 * ```
 * <a [routerLink]="['/user/bob']" preserveQueryParams preserveFragment>
 *   link to user component
 * </a>
 * ```
 *
 * You can tell the directive how to handle queryParams. Available options are:
 *  - `'merge'`: merge the queryParams into the current queryParams
 *  - `'preserve'`: preserve the current queryParams
 *  - default/`''`: use the queryParams only
 *
 * Same options for {\@link NavigationExtras#queryParamsHandling
 * NavigationExtras#queryParamsHandling}.
 *
 * ```
 * <a [routerLink]="['/user/bob']" [queryParams]="{debug: true}" queryParamsHandling="merge">
 *   link to user component
 * </a>
 * ```
 *
 * You can provide a `state` value to be persisted to the browser's History.state
 * property (See https://developer.mozilla.org/en-US/docs/Web/API/History#Properties). It's
 * used as follows:
 *
 * ```
 * <a [routerLink]="['/user/bob']" [state]="{tracingId: 123}">
 *   link to user component
 * </a>
 * ```
 *
 * And later the value can be read from the router through `router.getCurrentNavigation`.
 * For example, to capture the `tracingId` above during the `NavigationStart` event:
 *
 * ```
 * // Get NavigationStart events
 * router.events.pipe(filter(e => e instanceof NavigationStart)).subscribe(e => {
 *   const navigation = router.getCurrentNavigation();
 *   tracingService.trace({id: navigation.extras.state.tracingId});
 * });
 * ```
 *
 * The router link directive always treats the provided input as a delta to the current url.
 *
 * For instance, if the current url is `/user/(box//aux:team)`.
 *
 * Then the following link `<a [routerLink]="['/user/jim']">Jim</a>` will generate the link
 * `/user/(jim//aux:team)`.
 *
 * See {\@link Router#createUrlTree createUrlTree} for more information.
 *
 * \@ngModule RouterModule
 *
 * \@publicApi
 */
class RouterLink {
    /**
     * @param {?} router
     * @param {?} route
     * @param {?} tabIndex
     * @param {?} renderer
     * @param {?} el
     */
    constructor(router, route, tabIndex, renderer, el) {
        this.router = router;
        this.route = route;
        this.commands = [];
        if (tabIndex == null) {
            renderer.setAttribute(el.nativeElement, 'tabindex', '0');
        }
    }
    /**
     * @param {?} commands
     * @return {?}
     */
    set routerLink(commands) {
        if (commands != null) {
            this.commands = Array.isArray(commands) ? commands : [commands];
        }
        else {
            this.commands = [];
        }
    }
    /**
     * @deprecated 4.0.0 use `queryParamsHandling` instead.
     * @param {?} value
     * @return {?}
     */
    set preserveQueryParams(value) {
        if (isDevMode() && (/** @type {?} */ (console)) && (/** @type {?} */ (console.warn))) {
            console.warn('preserveQueryParams is deprecated!, use queryParamsHandling instead.');
        }
        this.preserve = value;
    }
    /**
     * @return {?}
     */
    onClick() {
        /** @type {?} */
        const extras = {
            skipLocationChange: attrBoolValue(this.skipLocationChange),
            replaceUrl: attrBoolValue(this.replaceUrl),
        };
        this.router.navigateByUrl(this.urlTree, extras);
        return true;
    }
    /**
     * @return {?}
     */
    get urlTree() {
        return this.router.createUrlTree(this.commands, {
            relativeTo: this.route,
            queryParams: this.queryParams,
            fragment: this.fragment,
            preserveQueryParams: attrBoolValue(this.preserve),
            queryParamsHandling: this.queryParamsHandling,
            preserveFragment: attrBoolValue(this.preserveFragment),
        });
    }
}
RouterLink.decorators = [
    { type: Directive, args: [{ selector: ':not(a):not(area)[routerLink]' },] }
];
/** @nocollapse */
RouterLink.ctorParameters = () => [
    { type: Router },
    { type: ActivatedRoute },
    { type: String, decorators: [{ type: Attribute, args: ['tabindex',] }] },
    { type: Renderer2 },
    { type: ElementRef }
];
RouterLink.propDecorators = {
    queryParams: [{ type: Input }],
    fragment: [{ type: Input }],
    queryParamsHandling: [{ type: Input }],
    preserveFragment: [{ type: Input }],
    skipLocationChange: [{ type: Input }],
    replaceUrl: [{ type: Input }],
    state: [{ type: Input }],
    routerLink: [{ type: Input }],
    preserveQueryParams: [{ type: Input }],
    onClick: [{ type: HostListener, args: ['click',] }]
};
if (false) {
    /** @type {?} */
    RouterLink.prototype.queryParams;
    /** @type {?} */
    RouterLink.prototype.fragment;
    /** @type {?} */
    RouterLink.prototype.queryParamsHandling;
    /** @type {?} */
    RouterLink.prototype.preserveFragment;
    /** @type {?} */
    RouterLink.prototype.skipLocationChange;
    /** @type {?} */
    RouterLink.prototype.replaceUrl;
    /** @type {?} */
    RouterLink.prototype.state;
    /**
     * @type {?}
     * @private
     */
    RouterLink.prototype.commands;
    /**
     * @type {?}
     * @private
     */
    RouterLink.prototype.preserve;
    /**
     * @type {?}
     * @private
     */
    RouterLink.prototype.router;
    /**
     * @type {?}
     * @private
     */
    RouterLink.prototype.route;
}
/**
 * \@description
 *
 * Lets you link to specific routes in your app.
 *
 * See `RouterLink` for more information.
 *
 * \@ngModule RouterModule
 *
 * \@publicApi
 */
class RouterLinkWithHref {
    /**
     * @param {?} router
     * @param {?} route
     * @param {?} locationStrategy
     */
    constructor(router, route, locationStrategy) {
        this.router = router;
        this.route = route;
        this.locationStrategy = locationStrategy;
        this.commands = [];
        this.subscription = router.events.subscribe((/**
         * @param {?} s
         * @return {?}
         */
        (s) => {
            if (s instanceof NavigationEnd) {
                this.updateTargetUrlAndHref();
            }
        }));
    }
    /**
     * @param {?} commands
     * @return {?}
     */
    set routerLink(commands) {
        if (commands != null) {
            this.commands = Array.isArray(commands) ? commands : [commands];
        }
        else {
            this.commands = [];
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set preserveQueryParams(value) {
        if (isDevMode() && (/** @type {?} */ (console)) && (/** @type {?} */ (console.warn))) {
            console.warn('preserveQueryParams is deprecated, use queryParamsHandling instead.');
        }
        this.preserve = value;
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) { this.updateTargetUrlAndHref(); }
    /**
     * @return {?}
     */
    ngOnDestroy() { this.subscription.unsubscribe(); }
    /**
     * @param {?} button
     * @param {?} ctrlKey
     * @param {?} metaKey
     * @param {?} shiftKey
     * @return {?}
     */
    onClick(button, ctrlKey, metaKey, shiftKey) {
        if (button !== 0 || ctrlKey || metaKey || shiftKey) {
            return true;
        }
        if (typeof this.target === 'string' && this.target != '_self') {
            return true;
        }
        /** @type {?} */
        const extras = {
            skipLocationChange: attrBoolValue(this.skipLocationChange),
            replaceUrl: attrBoolValue(this.replaceUrl),
            state: this.state
        };
        this.router.navigateByUrl(this.urlTree, extras);
        return false;
    }
    /**
     * @private
     * @return {?}
     */
    updateTargetUrlAndHref() {
        this.href = this.locationStrategy.prepareExternalUrl(this.router.serializeUrl(this.urlTree));
    }
    /**
     * @return {?}
     */
    get urlTree() {
        return this.router.createUrlTree(this.commands, {
            relativeTo: this.route,
            queryParams: this.queryParams,
            fragment: this.fragment,
            preserveQueryParams: attrBoolValue(this.preserve),
            queryParamsHandling: this.queryParamsHandling,
            preserveFragment: attrBoolValue(this.preserveFragment),
        });
    }
}
RouterLinkWithHref.decorators = [
    { type: Directive, args: [{ selector: 'a[routerLink],area[routerLink]' },] }
];
/** @nocollapse */
RouterLinkWithHref.ctorParameters = () => [
    { type: Router },
    { type: ActivatedRoute },
    { type: LocationStrategy }
];
RouterLinkWithHref.propDecorators = {
    target: [{ type: HostBinding, args: ['attr.target',] }, { type: Input }],
    queryParams: [{ type: Input }],
    fragment: [{ type: Input }],
    queryParamsHandling: [{ type: Input }],
    preserveFragment: [{ type: Input }],
    skipLocationChange: [{ type: Input }],
    replaceUrl: [{ type: Input }],
    state: [{ type: Input }],
    href: [{ type: HostBinding }],
    routerLink: [{ type: Input }],
    preserveQueryParams: [{ type: Input }],
    onClick: [{ type: HostListener, args: ['click', ['$event.button', '$event.ctrlKey', '$event.metaKey', '$event.shiftKey'],] }]
};
if (false) {
    /** @type {?} */
    RouterLinkWithHref.prototype.target;
    /** @type {?} */
    RouterLinkWithHref.prototype.queryParams;
    /** @type {?} */
    RouterLinkWithHref.prototype.fragment;
    /** @type {?} */
    RouterLinkWithHref.prototype.queryParamsHandling;
    /** @type {?} */
    RouterLinkWithHref.prototype.preserveFragment;
    /** @type {?} */
    RouterLinkWithHref.prototype.skipLocationChange;
    /** @type {?} */
    RouterLinkWithHref.prototype.replaceUrl;
    /** @type {?} */
    RouterLinkWithHref.prototype.state;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.commands;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.subscription;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.preserve;
    /** @type {?} */
    RouterLinkWithHref.prototype.href;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.router;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.route;
    /**
     * @type {?}
     * @private
     */
    RouterLinkWithHref.prototype.locationStrategy;
}
/**
 * @param {?} s
 * @return {?}
 */
function attrBoolValue(s) {
    return s === '' || !!s;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 *
 * \@description
 *
 * Lets you add a CSS class to an element when the link's route becomes active.
 *
 * This directive lets you add a CSS class to an element when the link's route
 * becomes active.
 *
 * Consider the following example:
 *
 * ```
 * <a routerLink="/user/bob" routerLinkActive="active-link">Bob</a>
 * ```
 *
 * When the url is either '/user' or '/user/bob', the active-link class will
 * be added to the `a` tag. If the url changes, the class will be removed.
 *
 * You can set more than one class, as follows:
 *
 * ```
 * <a routerLink="/user/bob" routerLinkActive="class1 class2">Bob</a>
 * <a routerLink="/user/bob" [routerLinkActive]="['class1', 'class2']">Bob</a>
 * ```
 *
 * You can configure RouterLinkActive by passing `exact: true`. This will add the classes
 * only when the url matches the link exactly.
 *
 * ```
 * <a routerLink="/user/bob" routerLinkActive="active-link" [routerLinkActiveOptions]="{exact:
 * true}">Bob</a>
 * ```
 *
 * You can assign the RouterLinkActive instance to a template variable and directly check
 * the `isActive` status.
 * ```
 * <a routerLink="/user/bob" routerLinkActive #rla="routerLinkActive">
 *   Bob {{ rla.isActive ? '(already open)' : ''}}
 * </a>
 * ```
 *
 * Finally, you can apply the RouterLinkActive directive to an ancestor of a RouterLink.
 *
 * ```
 * <div routerLinkActive="active-link" [routerLinkActiveOptions]="{exact: true}">
 *   <a routerLink="/user/jim">Jim</a>
 *   <a routerLink="/user/bob">Bob</a>
 * </div>
 * ```
 *
 * This will set the active-link class on the div tag if the url is either '/user/jim' or
 * '/user/bob'.
 *
 * \@ngModule RouterModule
 *
 * \@publicApi
 */
class RouterLinkActive {
    /**
     * @param {?} router
     * @param {?} element
     * @param {?} renderer
     * @param {?=} link
     * @param {?=} linkWithHref
     */
    constructor(router, element, renderer, link, linkWithHref) {
        this.router = router;
        this.element = element;
        this.renderer = renderer;
        this.link = link;
        this.linkWithHref = linkWithHref;
        this.classes = [];
        this.isActive = false;
        this.routerLinkActiveOptions = { exact: false };
        this.subscription = router.events.subscribe((/**
         * @param {?} s
         * @return {?}
         */
        (s) => {
            if (s instanceof NavigationEnd) {
                this.update();
            }
        }));
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.links.changes.subscribe((/**
         * @param {?} _
         * @return {?}
         */
        _ => this.update()));
        this.linksWithHrefs.changes.subscribe((/**
         * @param {?} _
         * @return {?}
         */
        _ => this.update()));
        this.update();
    }
    /**
     * @param {?} data
     * @return {?}
     */
    set routerLinkActive(data) {
        /** @type {?} */
        const classes = Array.isArray(data) ? data : data.split(' ');
        this.classes = classes.filter((/**
         * @param {?} c
         * @return {?}
         */
        c => !!c));
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) { this.update(); }
    /**
     * @return {?}
     */
    ngOnDestroy() { this.subscription.unsubscribe(); }
    /**
     * @private
     * @return {?}
     */
    update() {
        if (!this.links || !this.linksWithHrefs || !this.router.navigated)
            return;
        Promise.resolve().then((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const hasActiveLinks = this.hasActiveLinks();
            if (this.isActive !== hasActiveLinks) {
                ((/** @type {?} */ (this))).isActive = hasActiveLinks;
                this.classes.forEach((/**
                 * @param {?} c
                 * @return {?}
                 */
                (c) => {
                    if (hasActiveLinks) {
                        this.renderer.addClass(this.element.nativeElement, c);
                    }
                    else {
                        this.renderer.removeClass(this.element.nativeElement, c);
                    }
                }));
            }
        }));
    }
    /**
     * @private
     * @param {?} router
     * @return {?}
     */
    isLinkActive(router) {
        return (/**
         * @param {?} link
         * @return {?}
         */
        (link) => router.isActive(link.urlTree, this.routerLinkActiveOptions.exact));
    }
    /**
     * @private
     * @return {?}
     */
    hasActiveLinks() {
        /** @type {?} */
        const isActiveCheckFn = this.isLinkActive(this.router);
        return this.link && isActiveCheckFn(this.link) ||
            this.linkWithHref && isActiveCheckFn(this.linkWithHref) ||
            this.links.some(isActiveCheckFn) || this.linksWithHrefs.some(isActiveCheckFn);
    }
}
RouterLinkActive.decorators = [
    { type: Directive, args: [{
                selector: '[routerLinkActive]',
                exportAs: 'routerLinkActive',
            },] }
];
/** @nocollapse */
RouterLinkActive.ctorParameters = () => [
    { type: Router },
    { type: ElementRef },
    { type: Renderer2 },
    { type: RouterLink, decorators: [{ type: Optional }] },
    { type: RouterLinkWithHref, decorators: [{ type: Optional }] }
];
RouterLinkActive.propDecorators = {
    links: [{ type: ContentChildren, args: [RouterLink, { descendants: true },] }],
    linksWithHrefs: [{ type: ContentChildren, args: [RouterLinkWithHref, { descendants: true },] }],
    routerLinkActiveOptions: [{ type: Input }],
    routerLinkActive: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    RouterLinkActive.prototype.links;
    /** @type {?} */
    RouterLinkActive.prototype.linksWithHrefs;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.classes;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.subscription;
    /** @type {?} */
    RouterLinkActive.prototype.isActive;
    /** @type {?} */
    RouterLinkActive.prototype.routerLinkActiveOptions;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.router;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.element;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.link;
    /**
     * @type {?}
     * @private
     */
    RouterLinkActive.prototype.linkWithHref;
}

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
/**
 * Store contextual information about a `RouterOutlet`
 *
 * \@publicApi
 */
class OutletContext {
    constructor() {
        this.outlet = null;
        this.route = null;
        this.resolver = null;
        this.children = new ChildrenOutletContexts();
        this.attachRef = null;
    }
}
if (false) {
    /** @type {?} */
    OutletContext.prototype.outlet;
    /** @type {?} */
    OutletContext.prototype.route;
    /** @type {?} */
    OutletContext.prototype.resolver;
    /** @type {?} */
    OutletContext.prototype.children;
    /** @type {?} */
    OutletContext.prototype.attachRef;
}
/**
 * Store contextual information about the children (= nested) `RouterOutlet`
 *
 * \@publicApi
 */
class ChildrenOutletContexts {
    constructor() {
        // contexts for child outlets, by name.
        this.contexts = new Map();
    }
    /**
     * Called when a `RouterOutlet` directive is instantiated
     * @param {?} childName
     * @param {?} outlet
     * @return {?}
     */
    onChildOutletCreated(childName, outlet) {
        /** @type {?} */
        const context = this.getOrCreateContext(childName);
        context.outlet = outlet;
        this.contexts.set(childName, context);
    }
    /**
     * Called when a `RouterOutlet` directive is destroyed.
     * We need to keep the context as the outlet could be destroyed inside a NgIf and might be
     * re-created later.
     * @param {?} childName
     * @return {?}
     */
    onChildOutletDestroyed(childName) {
        /** @type {?} */
        const context = this.getContext(childName);
        if (context) {
            context.outlet = null;
        }
    }
    /**
     * Called when the corresponding route is deactivated during navigation.
     * Because the component get destroyed, all children outlet are destroyed.
     * @return {?}
     */
    onOutletDeactivated() {
        /** @type {?} */
        const contexts = this.contexts;
        this.contexts = new Map();
        return contexts;
    }
    /**
     * @param {?} contexts
     * @return {?}
     */
    onOutletReAttached(contexts) { this.contexts = contexts; }
    /**
     * @param {?} childName
     * @return {?}
     */
    getOrCreateContext(childName) {
        /** @type {?} */
        let context = this.getContext(childName);
        if (!context) {
            context = new OutletContext();
            this.contexts.set(childName, context);
        }
        return context;
    }
    /**
     * @param {?} childName
     * @return {?}
     */
    getContext(childName) { return this.contexts.get(childName) || null; }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ChildrenOutletContexts.prototype.contexts;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * \@description
 *
 * Acts as a placeholder that Angular dynamically fills based on the current router state.
 *
 * Each outlet can have a unique name, determined by the optional `name` attribute.
 * The name cannot be set or changed dynamically. If not set, default value is "primary".
 *
 * ```
 * <router-outlet></router-outlet>
 * <router-outlet name='left'></router-outlet>
 * <router-outlet name='right'></router-outlet>
 * ```
 *
 * A router outlet emits an activate event when a new component is instantiated,
 * and a deactivate event when a component is destroyed.
 *
 * ```
 * <router-outlet
 *   (activate)='onActivate($event)'
 *   (deactivate)='onDeactivate($event)'></router-outlet>
 * ```
 * \@ngModule RouterModule
 *
 * \@publicApi
 */
class RouterOutlet {
    /**
     * @param {?} parentContexts
     * @param {?} location
     * @param {?} resolver
     * @param {?} name
     * @param {?} changeDetector
     */
    constructor(parentContexts, location, resolver, name, changeDetector) {
        this.parentContexts = parentContexts;
        this.location = location;
        this.resolver = resolver;
        this.changeDetector = changeDetector;
        this.activated = null;
        this._activatedRoute = null;
        this.activateEvents = new EventEmitter();
        this.deactivateEvents = new EventEmitter();
        this.name = name || PRIMARY_OUTLET;
        parentContexts.onChildOutletCreated(this.name, this);
    }
    /**
     * @return {?}
     */
    ngOnDestroy() { this.parentContexts.onChildOutletDestroyed(this.name); }
    /**
     * @return {?}
     */
    ngOnInit() {
        if (!this.activated) {
            // If the outlet was not instantiated at the time the route got activated we need to populate
            // the outlet when it is initialized (ie inside a NgIf)
            /** @type {?} */
            const context = this.parentContexts.getContext(this.name);
            if (context && context.route) {
                if (context.attachRef) {
                    // `attachRef` is populated when there is an existing component to mount
                    this.attach(context.attachRef, context.route);
                }
                else {
                    // otherwise the component defined in the configuration is created
                    this.activateWith(context.route, context.resolver || null);
                }
            }
        }
    }
    /**
     * @return {?}
     */
    get isActivated() { return !!this.activated; }
    /**
     * @return {?}
     */
    get component() {
        if (!this.activated)
            throw new Error('Outlet is not activated');
        return this.activated.instance;
    }
    /**
     * @return {?}
     */
    get activatedRoute() {
        if (!this.activated)
            throw new Error('Outlet is not activated');
        return (/** @type {?} */ (this._activatedRoute));
    }
    /**
     * @return {?}
     */
    get activatedRouteData() {
        if (this._activatedRoute) {
            return this._activatedRoute.snapshot.data;
        }
        return {};
    }
    /**
     * Called when the `RouteReuseStrategy` instructs to detach the subtree
     * @return {?}
     */
    detach() {
        if (!this.activated)
            throw new Error('Outlet is not activated');
        this.location.detach();
        /** @type {?} */
        const cmp = this.activated;
        this.activated = null;
        this._activatedRoute = null;
        return cmp;
    }
    /**
     * Called when the `RouteReuseStrategy` instructs to re-attach a previously detached subtree
     * @param {?} ref
     * @param {?} activatedRoute
     * @return {?}
     */
    attach(ref, activatedRoute) {
        this.activated = ref;
        this._activatedRoute = activatedRoute;
        this.location.insert(ref.hostView);
    }
    /**
     * @return {?}
     */
    deactivate() {
        if (this.activated) {
            /** @type {?} */
            const c = this.component;
            this.activated.destroy();
            this.activated = null;
            this._activatedRoute = null;
            this.deactivateEvents.emit(c);
        }
    }
    /**
     * @param {?} activatedRoute
     * @param {?} resolver
     * @return {?}
     */
    activateWith(activatedRoute, resolver) {
        if (this.isActivated) {
            throw new Error('Cannot activate an already activated outlet');
        }
        this._activatedRoute = activatedRoute;
        /** @type {?} */
        const snapshot = activatedRoute._futureSnapshot;
        /** @type {?} */
        const component = (/** @type {?} */ ((/** @type {?} */ (snapshot.routeConfig)).component));
        resolver = resolver || this.resolver;
        /** @type {?} */
        const factory = resolver.resolveComponentFactory(component);
        /** @type {?} */
        const childContexts = this.parentContexts.getOrCreateContext(this.name).children;
        /** @type {?} */
        const injector = new OutletInjector(activatedRoute, childContexts, this.location.injector);
        this.activated = this.location.createComponent(factory, this.location.length, injector);
        // Calling `markForCheck` to make sure we will run the change detection when the
        // `RouterOutlet` is inside a `ChangeDetectionStrategy.OnPush` component.
        this.changeDetector.markForCheck();
        this.activateEvents.emit(this.activated.instance);
    }
}
RouterOutlet.decorators = [
    { type: Directive, args: [{ selector: 'router-outlet', exportAs: 'outlet' },] }
];
/** @nocollapse */
RouterOutlet.ctorParameters = () => [
    { type: ChildrenOutletContexts },
    { type: ViewContainerRef },
    { type: ComponentFactoryResolver },
    { type: String, decorators: [{ type: Attribute, args: ['name',] }] },
    { type: ChangeDetectorRef }
];
RouterOutlet.propDecorators = {
    activateEvents: [{ type: Output, args: ['activate',] }],
    deactivateEvents: [{ type: Output, args: ['deactivate',] }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.activated;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype._activatedRoute;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.name;
    /** @type {?} */
    RouterOutlet.prototype.activateEvents;
    /** @type {?} */
    RouterOutlet.prototype.deactivateEvents;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.parentContexts;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.location;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.resolver;
    /**
     * @type {?}
     * @private
     */
    RouterOutlet.prototype.changeDetector;
}
class OutletInjector {
    /**
     * @param {?} route
     * @param {?} childContexts
     * @param {?} parent
     */
    constructor(route, childContexts, parent) {
        this.route = route;
        this.childContexts = childContexts;
        this.parent = parent;
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @return {?}
     */
    get(token, notFoundValue) {
        if (token === ActivatedRoute) {
            return this.route;
        }
        if (token === ChildrenOutletContexts) {
            return this.childContexts;
        }
        return this.parent.get(token, notFoundValue);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    OutletInjector.prototype.route;
    /**
     * @type {?}
     * @private
     */
    OutletInjector.prototype.childContexts;
    /**
     * @type {?}
     * @private
     */
    OutletInjector.prototype.parent;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * \@description
 *
 * Provides a preloading strategy.
 *
 * \@publicApi
 * @abstract
 */
class PreloadingStrategy {
}
if (false) {
    /**
     * @abstract
     * @param {?} route
     * @param {?} fn
     * @return {?}
     */
    PreloadingStrategy.prototype.preload = function (route, fn) { };
}
/**
 * \@description
 *
 * Provides a preloading strategy that preloads all modules as quickly as possible.
 *
 * ```
 * RouteModule.forRoot(ROUTES, {preloadingStrategy: PreloadAllModules})
 * ```
 *
 * \@publicApi
 */
class PreloadAllModules {
    /**
     * @param {?} route
     * @param {?} fn
     * @return {?}
     */
    preload(route, fn) {
        return fn().pipe(catchError((/**
         * @return {?}
         */
        () => of(null))));
    }
}
/**
 * \@description
 *
 * Provides a preloading strategy that does not preload any modules.
 *
 * This strategy is enabled by default.
 *
 * \@publicApi
 */
class NoPreloading {
    /**
     * @param {?} route
     * @param {?} fn
     * @return {?}
     */
    preload(route, fn) { return of(null); }
}
/**
 * The preloader optimistically loads all router configurations to
 * make navigations into lazily-loaded sections of the application faster.
 *
 * The preloader runs in the background. When the router bootstraps, the preloader
 * starts listening to all navigation events. After every such event, the preloader
 * will check if any configurations can be loaded lazily.
 *
 * If a route is protected by `canLoad` guards, the preloaded will not load it.
 *
 * \@publicApi
 */
class RouterPreloader {
    /**
     * @param {?} router
     * @param {?} moduleLoader
     * @param {?} compiler
     * @param {?} injector
     * @param {?} preloadingStrategy
     */
    constructor(router, moduleLoader, compiler, injector, preloadingStrategy) {
        this.router = router;
        this.injector = injector;
        this.preloadingStrategy = preloadingStrategy;
        /** @type {?} */
        const onStartLoad = (/**
         * @param {?} r
         * @return {?}
         */
        (r) => router.triggerEvent(new RouteConfigLoadStart(r)));
        /** @type {?} */
        const onEndLoad = (/**
         * @param {?} r
         * @return {?}
         */
        (r) => router.triggerEvent(new RouteConfigLoadEnd(r)));
        this.loader = new RouterConfigLoader(moduleLoader, compiler, onStartLoad, onEndLoad);
    }
    /**
     * @return {?}
     */
    setUpPreloading() {
        this.subscription =
            this.router.events
                .pipe(filter((/**
             * @param {?} e
             * @return {?}
             */
            (e) => e instanceof NavigationEnd)), concatMap((/**
             * @return {?}
             */
            () => this.preload())))
                .subscribe((/**
             * @return {?}
             */
            () => { }));
    }
    /**
     * @return {?}
     */
    preload() {
        /** @type {?} */
        const ngModule = this.injector.get(NgModuleRef);
        return this.processRoutes(ngModule, this.router.config);
    }
    // TODO(jasonaden): This class relies on code external to the class to call setUpPreloading. If
    // this hasn't been done, ngOnDestroy will fail as this.subscription will be undefined. This
    // should be refactored.
    /**
     * @return {?}
     */
    ngOnDestroy() { this.subscription.unsubscribe(); }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} routes
     * @return {?}
     */
    processRoutes(ngModule, routes) {
        /** @type {?} */
        const res = [];
        for (const route of routes) {
            // we already have the config loaded, just recurse
            if (route.loadChildren && !route.canLoad && route._loadedConfig) {
                /** @type {?} */
                const childConfig = route._loadedConfig;
                res.push(this.processRoutes(childConfig.module, childConfig.routes));
                // no config loaded, fetch the config
            }
            else if (route.loadChildren && !route.canLoad) {
                res.push(this.preloadConfig(ngModule, route));
                // recurse into children
            }
            else if (route.children) {
                res.push(this.processRoutes(ngModule, route.children));
            }
        }
        return from(res).pipe(mergeAll(), map((/**
         * @param {?} _
         * @return {?}
         */
        (_) => void 0)));
    }
    /**
     * @private
     * @param {?} ngModule
     * @param {?} route
     * @return {?}
     */
    preloadConfig(ngModule, route) {
        return this.preloadingStrategy.preload(route, (/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const loaded$ = this.loader.load(ngModule.injector, route);
            return loaded$.pipe(mergeMap((/**
             * @param {?} config
             * @return {?}
             */
            (config) => {
                route._loadedConfig = config;
                return this.processRoutes(config.module, config.routes);
            })));
        }));
    }
}
RouterPreloader.decorators = [
    { type: Injectable }
];
/** @nocollapse */
RouterPreloader.ctorParameters = () => [
    { type: Router },
    { type: NgModuleFactoryLoader },
    { type: Compiler },
    { type: Injector },
    { type: PreloadingStrategy }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterPreloader.prototype.loader;
    /**
     * @type {?}
     * @private
     */
    RouterPreloader.prototype.subscription;
    /**
     * @type {?}
     * @private
     */
    RouterPreloader.prototype.router;
    /**
     * @type {?}
     * @private
     */
    RouterPreloader.prototype.injector;
    /**
     * @type {?}
     * @private
     */
    RouterPreloader.prototype.preloadingStrategy;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class RouterScroller {
    /**
     * @param {?} router
     * @param {?} viewportScroller
     * @param {?=} options
     */
    constructor(router, viewportScroller, options = {}) {
        this.router = router;
        this.viewportScroller = viewportScroller;
        this.options = options;
        this.lastId = 0;
        this.lastSource = 'imperative';
        this.restoredId = 0;
        this.store = {};
        // Default both options to 'disabled'
        options.scrollPositionRestoration = options.scrollPositionRestoration || 'disabled';
        options.anchorScrolling = options.anchorScrolling || 'disabled';
    }
    /**
     * @return {?}
     */
    init() {
        // we want to disable the automatic scrolling because having two places
        // responsible for scrolling results race conditions, especially given
        // that browser don't implement this behavior consistently
        if (this.options.scrollPositionRestoration !== 'disabled') {
            this.viewportScroller.setHistoryScrollRestoration('manual');
        }
        this.routerEventsSubscription = this.createScrollEvents();
        this.scrollEventsSubscription = this.consumeScrollEvents();
    }
    /**
     * @private
     * @return {?}
     */
    createScrollEvents() {
        return this.router.events.subscribe((/**
         * @param {?} e
         * @return {?}
         */
        e => {
            if (e instanceof NavigationStart) {
                // store the scroll position of the current stable navigations.
                this.store[this.lastId] = this.viewportScroller.getScrollPosition();
                this.lastSource = e.navigationTrigger;
                this.restoredId = e.restoredState ? e.restoredState.navigationId : 0;
            }
            else if (e instanceof NavigationEnd) {
                this.lastId = e.id;
                this.scheduleScrollEvent(e, this.router.parseUrl(e.urlAfterRedirects).fragment);
            }
        }));
    }
    /**
     * @private
     * @return {?}
     */
    consumeScrollEvents() {
        return this.router.events.subscribe((/**
         * @param {?} e
         * @return {?}
         */
        e => {
            if (!(e instanceof Scroll))
                return;
            // a popstate event. The pop state event will always ignore anchor scrolling.
            if (e.position) {
                if (this.options.scrollPositionRestoration === 'top') {
                    this.viewportScroller.scrollToPosition([0, 0]);
                }
                else if (this.options.scrollPositionRestoration === 'enabled') {
                    this.viewportScroller.scrollToPosition(e.position);
                }
                // imperative navigation "forward"
            }
            else {
                if (e.anchor && this.options.anchorScrolling === 'enabled') {
                    this.viewportScroller.scrollToAnchor(e.anchor);
                }
                else if (this.options.scrollPositionRestoration !== 'disabled') {
                    this.viewportScroller.scrollToPosition([0, 0]);
                }
            }
        }));
    }
    /**
     * @private
     * @param {?} routerEvent
     * @param {?} anchor
     * @return {?}
     */
    scheduleScrollEvent(routerEvent, anchor) {
        this.router.triggerEvent(new Scroll(routerEvent, this.lastSource === 'popstate' ? this.store[this.restoredId] : null, anchor));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        if (this.routerEventsSubscription) {
            this.routerEventsSubscription.unsubscribe();
        }
        if (this.scrollEventsSubscription) {
            this.scrollEventsSubscription.unsubscribe();
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.routerEventsSubscription;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.scrollEventsSubscription;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.lastId;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.lastSource;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.restoredId;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.store;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.router;
    /**
     * \@docsNotRequired
     * @type {?}
     */
    RouterScroller.prototype.viewportScroller;
    /**
     * @type {?}
     * @private
     */
    RouterScroller.prototype.options;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * The directives defined in the `RouterModule`.
 * @type {?}
 */
const ROUTER_DIRECTIVES = [RouterOutlet, RouterLink, RouterLinkWithHref, RouterLinkActive, ɵEmptyOutletComponent];
/**
 * A [DI token](guide/glossary/#di-token) for the router service.
 *
 * \@publicApi
 * @type {?}
 */
const ROUTER_CONFIGURATION = new InjectionToken('ROUTER_CONFIGURATION');
/**
 * \@docsNotRequired
 * @type {?}
 */
const ROUTER_FORROOT_GUARD = new InjectionToken('ROUTER_FORROOT_GUARD');
const ɵ0 = { enableTracing: false };
/** @type {?} */
const ROUTER_PROVIDERS = [
    Location,
    { provide: UrlSerializer, useClass: DefaultUrlSerializer },
    {
        provide: Router,
        useFactory: setupRouter,
        deps: [
            ApplicationRef, UrlSerializer, ChildrenOutletContexts, Location, Injector,
            NgModuleFactoryLoader, Compiler, ROUTES, ROUTER_CONFIGURATION,
            [UrlHandlingStrategy, new Optional()], [RouteReuseStrategy, new Optional()]
        ]
    },
    ChildrenOutletContexts,
    { provide: ActivatedRoute, useFactory: rootRoute, deps: [Router] },
    { provide: NgModuleFactoryLoader, useClass: SystemJsNgModuleLoader },
    RouterPreloader,
    NoPreloading,
    PreloadAllModules,
    { provide: ROUTER_CONFIGURATION, useValue: ɵ0 },
];
/**
 * @return {?}
 */
function routerNgProbeToken() {
    return new NgProbeToken('Router', Router);
}
/**
 * \@usageNotes
 *
 * RouterModule can be imported multiple times: once per lazily-loaded bundle.
 * Since the router deals with a global shared resource--location, we cannot have
 * more than one router service active.
 *
 * That is why there are two ways to create the module: `RouterModule.forRoot` and
 * `RouterModule.forChild`.
 *
 * * `forRoot` creates a module that contains all the directives, the given routes, and the router
 *   service itself.
 * * `forChild` creates a module that contains all the directives and the given routes, but does not
 *   include the router service.
 *
 * When registered at the root, the module should be used as follows
 *
 * ```
 * \@NgModule({
 *   imports: [RouterModule.forRoot(ROUTES)]
 * })
 * class MyNgModule {}
 * ```
 *
 * For submodules and lazy loaded submodules the module should be used as follows:
 *
 * ```
 * \@NgModule({
 *   imports: [RouterModule.forChild(ROUTES)]
 * })
 * class MyNgModule {}
 * ```
 *
 * \@description
 *
 * Adds router directives and providers.
 *
 * Managing state transitions is one of the hardest parts of building applications. This is
 * especially true on the web, where you also need to ensure that the state is reflected in the URL.
 * In addition, we often want to split applications into multiple bundles and load them on demand.
 * Doing this transparently is not trivial.
 *
 * The Angular router service solves these problems. Using the router, you can declaratively specify
 * application states, manage state transitions while taking care of the URL, and load bundles on
 * demand.
 *
 * @see [Routing and Navigation](guide/router.html) for an
 * overview of how the router service should be used.
 *
 * \@publicApi
 */
class RouterModule {
    // Note: We are injecting the Router so it gets created eagerly...
    /**
     * @param {?} guard
     * @param {?} router
     */
    constructor(guard, router) { }
    /**
     * Creates and configures a module with all the router providers and directives.
     * Optionally sets up an application listener to perform an initial navigation.
     *
     * @param {?} routes An array of `Route` objects that define the navigation paths for the application.
     * @param {?=} config An `ExtraOptions` configuration object that controls how navigation is performed.
     * @return {?} The new router module.
     */
    static forRoot(routes, config) {
        return {
            ngModule: RouterModule,
            providers: [
                ROUTER_PROVIDERS,
                provideRoutes(routes),
                {
                    provide: ROUTER_FORROOT_GUARD,
                    useFactory: provideForRootGuard,
                    deps: [[Router, new Optional(), new SkipSelf()]]
                },
                { provide: ROUTER_CONFIGURATION, useValue: config ? config : {} },
                {
                    provide: LocationStrategy,
                    useFactory: provideLocationStrategy,
                    deps: [
                        PlatformLocation, [new Inject(APP_BASE_HREF), new Optional()], ROUTER_CONFIGURATION
                    ]
                },
                {
                    provide: RouterScroller,
                    useFactory: createRouterScroller,
                    deps: [Router, ViewportScroller, ROUTER_CONFIGURATION]
                },
                {
                    provide: PreloadingStrategy,
                    useExisting: config && config.preloadingStrategy ? config.preloadingStrategy :
                        NoPreloading
                },
                { provide: NgProbeToken, multi: true, useFactory: routerNgProbeToken },
                provideRouterInitializer(),
            ],
        };
    }
    /**
     * Creates a module with all the router directives and a provider registering routes.
     * @param {?} routes
     * @return {?}
     */
    static forChild(routes) {
        return { ngModule: RouterModule, providers: [provideRoutes(routes)] };
    }
}
RouterModule.decorators = [
    { type: NgModule, args: [{
                declarations: ROUTER_DIRECTIVES,
                exports: ROUTER_DIRECTIVES,
                entryComponents: [ɵEmptyOutletComponent]
            },] }
];
/** @nocollapse */
RouterModule.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [ROUTER_FORROOT_GUARD,] }] },
    { type: Router, decorators: [{ type: Optional }] }
];
/**
 * @param {?} router
 * @param {?} viewportScroller
 * @param {?} config
 * @return {?}
 */
function createRouterScroller(router, viewportScroller, config) {
    if (config.scrollOffset) {
        viewportScroller.setOffset(config.scrollOffset);
    }
    return new RouterScroller(router, viewportScroller, config);
}
/**
 * @param {?} platformLocationStrategy
 * @param {?} baseHref
 * @param {?=} options
 * @return {?}
 */
function provideLocationStrategy(platformLocationStrategy, baseHref, options = {}) {
    return options.useHash ? new HashLocationStrategy(platformLocationStrategy, baseHref) :
        new PathLocationStrategy(platformLocationStrategy, baseHref);
}
/**
 * @param {?} router
 * @return {?}
 */
function provideForRootGuard(router) {
    if (router) {
        throw new Error(`RouterModule.forRoot() called twice. Lazy loaded modules should use RouterModule.forChild() instead.`);
    }
    return 'guarded';
}
/**
 * Registers a [DI provider](guide/glossary#provider) for a set of routes.
 * \@usageNotes
 *
 * ```
 * \@NgModule({
 *   imports: [RouterModule.forChild(ROUTES)],
 *   providers: [provideRoutes(EXTRA_ROUTES)]
 * })
 * class MyNgModule {}
 * ```
 *
 * \@publicApi
 * @param {?} routes The route configuration to provide.
 *
 * @return {?}
 */
function provideRoutes(routes) {
    return [
        { provide: ANALYZE_FOR_ENTRY_COMPONENTS, multi: true, useValue: routes },
        { provide: ROUTES, multi: true, useValue: routes },
    ];
}
/**
 * A set of configuration options for a router module, provided in the
 * `forRoot()` method.
 *
 * \@publicApi
 * @record
 */
function ExtraOptions() { }
if (false) {
    /**
     * When true, log all internal navigation events to the console.
     * Use for debugging.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.enableTracing;
    /**
     * When true, enable the location strategy that uses the URL fragment
     * instead of the history API.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.useHash;
    /**
     * One of `enabled` or `disabled`.
     * When set to `enabled`, the initial navigation starts before the root component is created.
     * The bootstrap is blocked until the initial navigation is complete. This value is required for
     * [server-side rendering](guide/universal) to work.
     * When set to `disabled`, the initial navigation is not performed.
     * The location listener is set up before the root component gets created.
     * Use if there is a reason to have more control over when the router
     * starts its initial navigation due to some complex initialization logic.
     *
     * Legacy values are deprecated since v4 and should not be used for new applications:
     *
     * * `legacy_enabled` - Default for compatibility.
     * The initial navigation starts after the root component has been created,
     * but the bootstrap is not blocked until the initial navigation is complete.
     * * `legacy_disabled` - The initial navigation is not performed.
     * The location listener is set up after the root component gets created.
     * * `true` - same as `legacy_enabled`.
     * * `false` - same as `legacy_disabled`.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.initialNavigation;
    /**
     * A custom error handler for failed navigations.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.errorHandler;
    /**
     * Configures a preloading strategy.
     * One of `PreloadAllModules` or `NoPreloading` (the default).
     * @type {?|undefined}
     */
    ExtraOptions.prototype.preloadingStrategy;
    /**
     * Define what the router should do if it receives a navigation request to the current URL.
     * Default is `ignore`, which causes the router ignores the navigation.
     * This can disable features such as a "refresh" button.
     * Use this option to configure the behavior when navigating to the
     * current URL. Default is 'ignore'.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.onSameUrlNavigation;
    /**
     * Configures if the scroll position needs to be restored when navigating back.
     *
     * * 'disabled'- (Default) Does nothing. Scroll position is maintained on navigation.
     * * 'top'- Sets the scroll position to x = 0, y = 0 on all navigation.
     * * 'enabled'- Restores the previous scroll position on backward navigation, else sets the
     * position to the anchor if one is provided, or sets the scroll position to [0, 0] (forward
     * navigation). This option will be the default in the future.
     *
     * You can implement custom scroll restoration behavior by adapting the enabled behavior as
     * in the following example.
     *
     * ```typescript
     * class AppModule {
     *   constructor(router: Router, viewportScroller: ViewportScroller) {
     *     router.events.pipe(
     *       filter((e: Event): e is Scroll => e instanceof Scroll)
     *     ).subscribe(e => {
     *       if (e.position) {
     *         // backward navigation
     *         viewportScroller.scrollToPosition(e.position);
     *       } else if (e.anchor) {
     *         // anchor navigation
     *         viewportScroller.scrollToAnchor(e.anchor);
     *       } else {
     *         // forward navigation
     *         viewportScroller.scrollToPosition([0, 0]);
     *       }
     *     });
     *   }
     * }
     * ```
     * @type {?|undefined}
     */
    ExtraOptions.prototype.scrollPositionRestoration;
    /**
     * When set to 'enabled', scrolls to the anchor element when the URL has a fragment.
     * Anchor scrolling is disabled by default.
     *
     * Anchor scrolling does not happen on 'popstate'. Instead, we restore the position
     * that we stored or scroll to the top.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.anchorScrolling;
    /**
     * Configures the scroll offset the router will use when scrolling to an element.
     *
     * When given a tuple with x and y position value,
     * the router uses that offset each time it scrolls.
     * When given a function, the router invokes the function every time
     * it restores scroll position.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.scrollOffset;
    /**
     * Defines how the router merges parameters, data, and resolved data from parent to child
     * routes. By default ('emptyOnly'), inherits parent parameters only for
     * path-less or component-less routes.
     * Set to 'always' to enable unconditional inheritance of parent parameters.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.paramsInheritanceStrategy;
    /**
     * A custom handler for malformed URI errors. The handler is invoked when `encodedURI` contains
     * invalid character sequences.
     * The default implementation is to redirect to the root URL, dropping
     * any path or parameter information. The function takes three parameters:
     *
     * - `'URIError'` - Error thrown when parsing a bad URL.
     * - `'UrlSerializer'` - UrlSerializer that’s configured with the router.
     * - `'url'` -  The malformed URL that caused the URIError
     *
     * @type {?|undefined}
     */
    ExtraOptions.prototype.malformedUriErrorHandler;
    /**
     * Defines when the router updates the browser URL. By default ('deferred'),
     * update after successful navigation.
     * Set to 'eager' if prefer to update the URL at the beginning of navigation.
     * Updating the URL early allows you to handle a failure of navigation by
     * showing an error message with the URL that failed.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.urlUpdateStrategy;
    /**
     * Enables a bug fix that corrects relative link resolution in components with empty paths.
     * Example:
     *
     * ```
     * const routes = [
     *   {
     *     path: '',
     *     component: ContainerComponent,
     *     children: [
     *       { path: 'a', component: AComponent },
     *       { path: 'b', component: BComponent },
     *     ]
     *   }
     * ];
     * ```
     *
     * From the `ContainerComponent`, this will not work:
     *
     * `<a [routerLink]="['./a']">Link to A</a>`
     *
     * However, this will work:
     *
     * `<a [routerLink]="['../a']">Link to A</a>`
     *
     * In other words, you're required to use `../` rather than `./`. This is currently the default
     * behavior. Setting this option to `corrected` enables the fix.
     * @type {?|undefined}
     */
    ExtraOptions.prototype.relativeLinkResolution;
}
/**
 * @param {?} ref
 * @param {?} urlSerializer
 * @param {?} contexts
 * @param {?} location
 * @param {?} injector
 * @param {?} loader
 * @param {?} compiler
 * @param {?} config
 * @param {?=} opts
 * @param {?=} urlHandlingStrategy
 * @param {?=} routeReuseStrategy
 * @return {?}
 */
function setupRouter(ref, urlSerializer, contexts, location, injector, loader, compiler, config, opts = {}, urlHandlingStrategy, routeReuseStrategy) {
    /** @type {?} */
    const router = new Router(null, urlSerializer, contexts, location, injector, loader, compiler, flatten(config));
    if (urlHandlingStrategy) {
        router.urlHandlingStrategy = urlHandlingStrategy;
    }
    if (routeReuseStrategy) {
        router.routeReuseStrategy = routeReuseStrategy;
    }
    if (opts.errorHandler) {
        router.errorHandler = opts.errorHandler;
    }
    if (opts.malformedUriErrorHandler) {
        router.malformedUriErrorHandler = opts.malformedUriErrorHandler;
    }
    if (opts.enableTracing) {
        /** @type {?} */
        const dom = ɵgetDOM();
        router.events.subscribe((/**
         * @param {?} e
         * @return {?}
         */
        (e) => {
            dom.logGroup(`Router Event: ${((/** @type {?} */ (e.constructor))).name}`);
            dom.log(e.toString());
            dom.log(e);
            dom.logGroupEnd();
        }));
    }
    if (opts.onSameUrlNavigation) {
        router.onSameUrlNavigation = opts.onSameUrlNavigation;
    }
    if (opts.paramsInheritanceStrategy) {
        router.paramsInheritanceStrategy = opts.paramsInheritanceStrategy;
    }
    if (opts.urlUpdateStrategy) {
        router.urlUpdateStrategy = opts.urlUpdateStrategy;
    }
    if (opts.relativeLinkResolution) {
        router.relativeLinkResolution = opts.relativeLinkResolution;
    }
    return router;
}
/**
 * @param {?} router
 * @return {?}
 */
function rootRoute(router) {
    return router.routerState.root;
}
/**
 * Router initialization requires two steps:
 *
 * First, we start the navigation in a `APP_INITIALIZER` to block the bootstrap if
 * a resolver or a guard executes asynchronously.
 *
 * Next, we actually run activation in a `BOOTSTRAP_LISTENER`, using the
 * `afterPreactivation` hook provided by the router.
 * The router navigation starts, reaches the point when preactivation is done, and then
 * pauses. It waits for the hook to be resolved. We then resolve it only in a bootstrap listener.
 */
class RouterInitializer {
    /**
     * @param {?} injector
     */
    constructor(injector) {
        this.injector = injector;
        this.initNavigation = false;
        this.resultOfPreactivationDone = new Subject();
    }
    /**
     * @return {?}
     */
    appInitializer() {
        /** @type {?} */
        const p = this.injector.get(LOCATION_INITIALIZED, Promise.resolve(null));
        return p.then((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            let resolve = (/** @type {?} */ (null));
            /** @type {?} */
            const res = new Promise((/**
             * @param {?} r
             * @return {?}
             */
            r => resolve = r));
            /** @type {?} */
            const router = this.injector.get(Router);
            /** @type {?} */
            const opts = this.injector.get(ROUTER_CONFIGURATION);
            if (this.isLegacyDisabled(opts) || this.isLegacyEnabled(opts)) {
                resolve(true);
            }
            else if (opts.initialNavigation === 'disabled') {
                router.setUpLocationChangeListener();
                resolve(true);
            }
            else if (opts.initialNavigation === 'enabled') {
                router.hooks.afterPreactivation = (/**
                 * @return {?}
                 */
                () => {
                    // only the initial navigation should be delayed
                    if (!this.initNavigation) {
                        this.initNavigation = true;
                        resolve(true);
                        return this.resultOfPreactivationDone;
                        // subsequent navigations should not be delayed
                    }
                    else {
                        return (/** @type {?} */ (of(null)));
                    }
                });
                router.initialNavigation();
            }
            else {
                throw new Error(`Invalid initialNavigation options: '${opts.initialNavigation}'`);
            }
            return res;
        }));
    }
    /**
     * @param {?} bootstrappedComponentRef
     * @return {?}
     */
    bootstrapListener(bootstrappedComponentRef) {
        /** @type {?} */
        const opts = this.injector.get(ROUTER_CONFIGURATION);
        /** @type {?} */
        const preloader = this.injector.get(RouterPreloader);
        /** @type {?} */
        const routerScroller = this.injector.get(RouterScroller);
        /** @type {?} */
        const router = this.injector.get(Router);
        /** @type {?} */
        const ref = this.injector.get(ApplicationRef);
        if (bootstrappedComponentRef !== ref.components[0]) {
            return;
        }
        if (this.isLegacyEnabled(opts)) {
            router.initialNavigation();
        }
        else if (this.isLegacyDisabled(opts)) {
            router.setUpLocationChangeListener();
        }
        preloader.setUpPreloading();
        routerScroller.init();
        router.resetRootComponentType(ref.componentTypes[0]);
        this.resultOfPreactivationDone.next((/** @type {?} */ (null)));
        this.resultOfPreactivationDone.complete();
    }
    /**
     * @private
     * @param {?} opts
     * @return {?}
     */
    isLegacyEnabled(opts) {
        return opts.initialNavigation === 'legacy_enabled' || opts.initialNavigation === true ||
            opts.initialNavigation === undefined;
    }
    /**
     * @private
     * @param {?} opts
     * @return {?}
     */
    isLegacyDisabled(opts) {
        return opts.initialNavigation === 'legacy_disabled' || opts.initialNavigation === false;
    }
}
RouterInitializer.decorators = [
    { type: Injectable }
];
/** @nocollapse */
RouterInitializer.ctorParameters = () => [
    { type: Injector }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterInitializer.prototype.initNavigation;
    /**
     * @type {?}
     * @private
     */
    RouterInitializer.prototype.resultOfPreactivationDone;
    /**
     * @type {?}
     * @private
     */
    RouterInitializer.prototype.injector;
}
/**
 * @param {?} r
 * @return {?}
 */
function getAppInitializer(r) {
    return r.appInitializer.bind(r);
}
/**
 * @param {?} r
 * @return {?}
 */
function getBootstrapListener(r) {
    return r.bootstrapListener.bind(r);
}
/**
 * A [DI token](guide/glossary/#di-token) for the router initializer that
 * is called after the app is bootstrapped.
 *
 * \@publicApi
 * @type {?}
 */
const ROUTER_INITIALIZER = new InjectionToken('Router Initializer');
/**
 * @return {?}
 */
function provideRouterInitializer() {
    return [
        RouterInitializer,
        {
            provide: APP_INITIALIZER,
            multi: true,
            useFactory: getAppInitializer,
            deps: [RouterInitializer]
        },
        { provide: ROUTER_INITIALIZER, useFactory: getBootstrapListener, deps: [RouterInitializer] },
        { provide: APP_BOOTSTRAP_LISTENER, multi: true, useExisting: ROUTER_INITIALIZER },
    ];
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * \@publicApi
 * @type {?}
 */
const VERSION = new Version('8.2.14');

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * Generated bundle index. Do not edit.
 */

export { ɵEmptyOutletComponent as ɵangular_packages_router_router_l, ɵEmptyOutletComponent, ROUTER_FORROOT_GUARD as ɵangular_packages_router_router_a, RouterInitializer as ɵangular_packages_router_router_h, createRouterScroller as ɵangular_packages_router_router_c, getAppInitializer as ɵangular_packages_router_router_i, getBootstrapListener as ɵangular_packages_router_router_j, provideForRootGuard as ɵangular_packages_router_router_e, provideLocationStrategy as ɵangular_packages_router_router_d, provideRouterInitializer as ɵangular_packages_router_router_k, rootRoute as ɵangular_packages_router_router_g, routerNgProbeToken as ɵangular_packages_router_router_b, setupRouter as ɵangular_packages_router_router_f, RouterScroller as ɵangular_packages_router_router_o, Tree as ɵangular_packages_router_router_m, TreeNode as ɵangular_packages_router_router_n, RouterLink, RouterLinkWithHref, RouterLinkActive, RouterOutlet, ActivationEnd, ActivationStart, ChildActivationEnd, ChildActivationStart, GuardsCheckEnd, GuardsCheckStart, NavigationCancel, NavigationEnd, NavigationError, NavigationStart, ResolveEnd, ResolveStart, RouteConfigLoadEnd, RouteConfigLoadStart, RouterEvent, RoutesRecognized, Scroll, RouteReuseStrategy, Router, ROUTES, ROUTER_CONFIGURATION, ROUTER_INITIALIZER, RouterModule, provideRoutes, ChildrenOutletContexts, OutletContext, NoPreloading, PreloadAllModules, PreloadingStrategy, RouterPreloader, ActivatedRoute, ActivatedRouteSnapshot, RouterState, RouterStateSnapshot, PRIMARY_OUTLET, convertToParamMap, UrlHandlingStrategy, DefaultUrlSerializer, UrlSegment, UrlSegmentGroup, UrlSerializer, UrlTree, VERSION, ROUTER_PROVIDERS as ɵROUTER_PROVIDERS, flatten as ɵflatten };
//# sourceMappingURL=router.js.map
