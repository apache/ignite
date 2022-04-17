/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
/**
 * Base for events the router goes through, as opposed to events tied to a specific
 * route. Fired one time for any given navigation.
 *
 * @usageNotes
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
 * @publicApi
 */
var RouterEvent = /** @class */ (function () {
    function RouterEvent(
    /** A unique ID that the router assigns to every router navigation. */
    id, 
    /** The URL that is the destination for this navigation. */
    url) {
        this.id = id;
        this.url = url;
    }
    return RouterEvent;
}());
export { RouterEvent };
/**
 * An event triggered when a navigation starts.
 *
 * @publicApi
 */
var NavigationStart = /** @class */ (function (_super) {
    tslib_1.__extends(NavigationStart, _super);
    function NavigationStart(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    navigationTrigger, 
    /** @docsNotRequired */
    restoredState) {
        if (navigationTrigger === void 0) { navigationTrigger = 'imperative'; }
        if (restoredState === void 0) { restoredState = null; }
        var _this = _super.call(this, id, url) || this;
        _this.navigationTrigger = navigationTrigger;
        _this.restoredState = restoredState;
        return _this;
    }
    /** @docsNotRequired */
    NavigationStart.prototype.toString = function () { return "NavigationStart(id: " + this.id + ", url: '" + this.url + "')"; };
    return NavigationStart;
}(RouterEvent));
export { NavigationStart };
/**
 * An event triggered when a navigation ends successfully.
 *
 * @publicApi
 */
var NavigationEnd = /** @class */ (function (_super) {
    tslib_1.__extends(NavigationEnd, _super);
    function NavigationEnd(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        return _this;
    }
    /** @docsNotRequired */
    NavigationEnd.prototype.toString = function () {
        return "NavigationEnd(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "')";
    };
    return NavigationEnd;
}(RouterEvent));
export { NavigationEnd };
/**
 * An event triggered when a navigation is canceled, directly or indirectly.
 *
 * This can happen when a [route guard](guide/router#milestone-5-route-guards)
 * returns `false` or initiates a redirect by returning a `UrlTree`.
 *
 * @publicApi
 */
var NavigationCancel = /** @class */ (function (_super) {
    tslib_1.__extends(NavigationCancel, _super);
    function NavigationCancel(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    reason) {
        var _this = _super.call(this, id, url) || this;
        _this.reason = reason;
        return _this;
    }
    /** @docsNotRequired */
    NavigationCancel.prototype.toString = function () { return "NavigationCancel(id: " + this.id + ", url: '" + this.url + "')"; };
    return NavigationCancel;
}(RouterEvent));
export { NavigationCancel };
/**
 * An event triggered when a navigation fails due to an unexpected error.
 *
 * @publicApi
 */
var NavigationError = /** @class */ (function (_super) {
    tslib_1.__extends(NavigationError, _super);
    function NavigationError(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    error) {
        var _this = _super.call(this, id, url) || this;
        _this.error = error;
        return _this;
    }
    /** @docsNotRequired */
    NavigationError.prototype.toString = function () {
        return "NavigationError(id: " + this.id + ", url: '" + this.url + "', error: " + this.error + ")";
    };
    return NavigationError;
}(RouterEvent));
export { NavigationError };
/**
 *An event triggered when routes are recognized.
 *
 * @publicApi
 */
var RoutesRecognized = /** @class */ (function (_super) {
    tslib_1.__extends(RoutesRecognized, _super);
    function RoutesRecognized(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects, 
    /** @docsNotRequired */
    state) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        _this.state = state;
        return _this;
    }
    /** @docsNotRequired */
    RoutesRecognized.prototype.toString = function () {
        return "RoutesRecognized(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "', state: " + this.state + ")";
    };
    return RoutesRecognized;
}(RouterEvent));
export { RoutesRecognized };
/**
 * An event triggered at the start of the Guard phase of routing.
 *
 * @publicApi
 */
var GuardsCheckStart = /** @class */ (function (_super) {
    tslib_1.__extends(GuardsCheckStart, _super);
    function GuardsCheckStart(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects, 
    /** @docsNotRequired */
    state) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        _this.state = state;
        return _this;
    }
    GuardsCheckStart.prototype.toString = function () {
        return "GuardsCheckStart(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "', state: " + this.state + ")";
    };
    return GuardsCheckStart;
}(RouterEvent));
export { GuardsCheckStart };
/**
 * An event triggered at the end of the Guard phase of routing.
 *
 * @publicApi
 */
var GuardsCheckEnd = /** @class */ (function (_super) {
    tslib_1.__extends(GuardsCheckEnd, _super);
    function GuardsCheckEnd(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects, 
    /** @docsNotRequired */
    state, 
    /** @docsNotRequired */
    shouldActivate) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        _this.state = state;
        _this.shouldActivate = shouldActivate;
        return _this;
    }
    GuardsCheckEnd.prototype.toString = function () {
        return "GuardsCheckEnd(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "', state: " + this.state + ", shouldActivate: " + this.shouldActivate + ")";
    };
    return GuardsCheckEnd;
}(RouterEvent));
export { GuardsCheckEnd };
/**
 * An event triggered at the the start of the Resolve phase of routing.
 *
 * Runs in the "resolve" phase whether or not there is anything to resolve.
 * In future, may change to only run when there are things to be resolved.
 *
 * @publicApi
 */
var ResolveStart = /** @class */ (function (_super) {
    tslib_1.__extends(ResolveStart, _super);
    function ResolveStart(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects, 
    /** @docsNotRequired */
    state) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        _this.state = state;
        return _this;
    }
    ResolveStart.prototype.toString = function () {
        return "ResolveStart(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "', state: " + this.state + ")";
    };
    return ResolveStart;
}(RouterEvent));
export { ResolveStart };
/**
 * An event triggered at the end of the Resolve phase of routing.
 * @see `ResolveStart`.
 *
 * @publicApi
 */
var ResolveEnd = /** @class */ (function (_super) {
    tslib_1.__extends(ResolveEnd, _super);
    function ResolveEnd(
    /** @docsNotRequired */
    id, 
    /** @docsNotRequired */
    url, 
    /** @docsNotRequired */
    urlAfterRedirects, 
    /** @docsNotRequired */
    state) {
        var _this = _super.call(this, id, url) || this;
        _this.urlAfterRedirects = urlAfterRedirects;
        _this.state = state;
        return _this;
    }
    ResolveEnd.prototype.toString = function () {
        return "ResolveEnd(id: " + this.id + ", url: '" + this.url + "', urlAfterRedirects: '" + this.urlAfterRedirects + "', state: " + this.state + ")";
    };
    return ResolveEnd;
}(RouterEvent));
export { ResolveEnd };
/**
 * An event triggered before lazy loading a route configuration.
 *
 * @publicApi
 */
var RouteConfigLoadStart = /** @class */ (function () {
    function RouteConfigLoadStart(
    /** @docsNotRequired */
    route) {
        this.route = route;
    }
    RouteConfigLoadStart.prototype.toString = function () { return "RouteConfigLoadStart(path: " + this.route.path + ")"; };
    return RouteConfigLoadStart;
}());
export { RouteConfigLoadStart };
/**
 * An event triggered when a route has been lazy loaded.
 *
 * @publicApi
 */
var RouteConfigLoadEnd = /** @class */ (function () {
    function RouteConfigLoadEnd(
    /** @docsNotRequired */
    route) {
        this.route = route;
    }
    RouteConfigLoadEnd.prototype.toString = function () { return "RouteConfigLoadEnd(path: " + this.route.path + ")"; };
    return RouteConfigLoadEnd;
}());
export { RouteConfigLoadEnd };
/**
 * An event triggered at the start of the child-activation
 * part of the Resolve phase of routing.
 * @see  `ChildActivationEnd`
 * @see `ResolveStart`
 *
 * @publicApi
 */
var ChildActivationStart = /** @class */ (function () {
    function ChildActivationStart(
    /** @docsNotRequired */
    snapshot) {
        this.snapshot = snapshot;
    }
    ChildActivationStart.prototype.toString = function () {
        var path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return "ChildActivationStart(path: '" + path + "')";
    };
    return ChildActivationStart;
}());
export { ChildActivationStart };
/**
 * An event triggered at the end of the child-activation part
 * of the Resolve phase of routing.
 * @see `ChildActivationStart`
 * @see `ResolveStart` *
 * @publicApi
 */
var ChildActivationEnd = /** @class */ (function () {
    function ChildActivationEnd(
    /** @docsNotRequired */
    snapshot) {
        this.snapshot = snapshot;
    }
    ChildActivationEnd.prototype.toString = function () {
        var path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return "ChildActivationEnd(path: '" + path + "')";
    };
    return ChildActivationEnd;
}());
export { ChildActivationEnd };
/**
 * An event triggered at the start of the activation part
 * of the Resolve phase of routing.
 * @see ActivationEnd`
 * @see `ResolveStart`
 *
 * @publicApi
 */
var ActivationStart = /** @class */ (function () {
    function ActivationStart(
    /** @docsNotRequired */
    snapshot) {
        this.snapshot = snapshot;
    }
    ActivationStart.prototype.toString = function () {
        var path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return "ActivationStart(path: '" + path + "')";
    };
    return ActivationStart;
}());
export { ActivationStart };
/**
 * An event triggered at the end of the activation part
 * of the Resolve phase of routing.
 * @see `ActivationStart`
 * @see `ResolveStart`
 *
 * @publicApi
 */
var ActivationEnd = /** @class */ (function () {
    function ActivationEnd(
    /** @docsNotRequired */
    snapshot) {
        this.snapshot = snapshot;
    }
    ActivationEnd.prototype.toString = function () {
        var path = this.snapshot.routeConfig && this.snapshot.routeConfig.path || '';
        return "ActivationEnd(path: '" + path + "')";
    };
    return ActivationEnd;
}());
export { ActivationEnd };
/**
 * An event triggered by scrolling.
 *
 * @publicApi
 */
var Scroll = /** @class */ (function () {
    function Scroll(
    /** @docsNotRequired */
    routerEvent, 
    /** @docsNotRequired */
    position, 
    /** @docsNotRequired */
    anchor) {
        this.routerEvent = routerEvent;
        this.position = position;
        this.anchor = anchor;
    }
    Scroll.prototype.toString = function () {
        var pos = this.position ? this.position[0] + ", " + this.position[1] : null;
        return "Scroll(anchor: '" + this.anchor + "', position: '" + pos + "')";
    };
    return Scroll;
}());
export { Scroll };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXZlbnRzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9ldmVudHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQWdCSDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7R0FvQkc7QUFDSDtJQUNFO0lBQ0ksc0VBQXNFO0lBQy9ELEVBQVU7SUFDakIsMkRBQTJEO0lBQ3BELEdBQVc7UUFGWCxPQUFFLEdBQUYsRUFBRSxDQUFRO1FBRVYsUUFBRyxHQUFILEdBQUcsQ0FBUTtJQUFHLENBQUM7SUFDNUIsa0JBQUM7QUFBRCxDQUFDLEFBTkQsSUFNQzs7QUFFRDs7OztHQUlHO0FBQ0g7SUFBcUMsMkNBQVc7SUEyQjlDO0lBQ0ksdUJBQXVCO0lBQ3ZCLEVBQVU7SUFDVix1QkFBdUI7SUFDdkIsR0FBVztJQUNYLHVCQUF1QjtJQUN2QixpQkFBc0U7SUFDdEUsdUJBQXVCO0lBQ3ZCLGFBQW1FO1FBRm5FLGtDQUFBLEVBQUEsZ0NBQXNFO1FBRXRFLDhCQUFBLEVBQUEsb0JBQW1FO1FBUnZFLFlBU0Usa0JBQU0sRUFBRSxFQUFFLEdBQUcsQ0FBQyxTQUdmO1FBRkMsS0FBSSxDQUFDLGlCQUFpQixHQUFHLGlCQUFpQixDQUFDO1FBQzNDLEtBQUksQ0FBQyxhQUFhLEdBQUcsYUFBYSxDQUFDOztJQUNyQyxDQUFDO0lBRUQsdUJBQXVCO0lBQ3ZCLGtDQUFRLEdBQVIsY0FBcUIsT0FBTyx5QkFBdUIsSUFBSSxDQUFDLEVBQUUsZ0JBQVcsSUFBSSxDQUFDLEdBQUcsT0FBSSxDQUFDLENBQUMsQ0FBQztJQUN0RixzQkFBQztBQUFELENBQUMsQUEzQ0QsQ0FBcUMsV0FBVyxHQTJDL0M7O0FBRUQ7Ozs7R0FJRztBQUNIO0lBQW1DLHlDQUFXO0lBQzVDO0lBQ0ksdUJBQXVCO0lBQ3ZCLEVBQVU7SUFDVix1QkFBdUI7SUFDdkIsR0FBVztJQUNYLHVCQUF1QjtJQUNoQixpQkFBeUI7UUFOcEMsWUFPRSxrQkFBTSxFQUFFLEVBQUUsR0FBRyxDQUFDLFNBQ2Y7UUFGVSx1QkFBaUIsR0FBakIsaUJBQWlCLENBQVE7O0lBRXBDLENBQUM7SUFFRCx1QkFBdUI7SUFDdkIsZ0NBQVEsR0FBUjtRQUNFLE9BQU8sdUJBQXFCLElBQUksQ0FBQyxFQUFFLGdCQUFXLElBQUksQ0FBQyxHQUFHLCtCQUEwQixJQUFJLENBQUMsaUJBQWlCLE9BQUksQ0FBQztJQUM3RyxDQUFDO0lBQ0gsb0JBQUM7QUFBRCxDQUFDLEFBZkQsQ0FBbUMsV0FBVyxHQWU3Qzs7QUFFRDs7Ozs7OztHQU9HO0FBQ0g7SUFBc0MsNENBQVc7SUFDL0M7SUFDSSx1QkFBdUI7SUFDdkIsRUFBVTtJQUNWLHVCQUF1QjtJQUN2QixHQUFXO0lBQ1gsdUJBQXVCO0lBQ2hCLE1BQWM7UUFOekIsWUFPRSxrQkFBTSxFQUFFLEVBQUUsR0FBRyxDQUFDLFNBQ2Y7UUFGVSxZQUFNLEdBQU4sTUFBTSxDQUFROztJQUV6QixDQUFDO0lBRUQsdUJBQXVCO0lBQ3ZCLG1DQUFRLEdBQVIsY0FBcUIsT0FBTywwQkFBd0IsSUFBSSxDQUFDLEVBQUUsZ0JBQVcsSUFBSSxDQUFDLEdBQUcsT0FBSSxDQUFDLENBQUMsQ0FBQztJQUN2Rix1QkFBQztBQUFELENBQUMsQUFiRCxDQUFzQyxXQUFXLEdBYWhEOztBQUVEOzs7O0dBSUc7QUFDSDtJQUFxQywyQ0FBVztJQUM5QztJQUNJLHVCQUF1QjtJQUN2QixFQUFVO0lBQ1YsdUJBQXVCO0lBQ3ZCLEdBQVc7SUFDWCx1QkFBdUI7SUFDaEIsS0FBVTtRQU5yQixZQU9FLGtCQUFNLEVBQUUsRUFBRSxHQUFHLENBQUMsU0FDZjtRQUZVLFdBQUssR0FBTCxLQUFLLENBQUs7O0lBRXJCLENBQUM7SUFFRCx1QkFBdUI7SUFDdkIsa0NBQVEsR0FBUjtRQUNFLE9BQU8seUJBQXVCLElBQUksQ0FBQyxFQUFFLGdCQUFXLElBQUksQ0FBQyxHQUFHLGtCQUFhLElBQUksQ0FBQyxLQUFLLE1BQUcsQ0FBQztJQUNyRixDQUFDO0lBQ0gsc0JBQUM7QUFBRCxDQUFDLEFBZkQsQ0FBcUMsV0FBVyxHQWUvQzs7QUFFRDs7OztHQUlHO0FBQ0g7SUFBc0MsNENBQVc7SUFDL0M7SUFDSSx1QkFBdUI7SUFDdkIsRUFBVTtJQUNWLHVCQUF1QjtJQUN2QixHQUFXO0lBQ1gsdUJBQXVCO0lBQ2hCLGlCQUF5QjtJQUNoQyx1QkFBdUI7SUFDaEIsS0FBMEI7UUFSckMsWUFTRSxrQkFBTSxFQUFFLEVBQUUsR0FBRyxDQUFDLFNBQ2Y7UUFKVSx1QkFBaUIsR0FBakIsaUJBQWlCLENBQVE7UUFFekIsV0FBSyxHQUFMLEtBQUssQ0FBcUI7O0lBRXJDLENBQUM7SUFFRCx1QkFBdUI7SUFDdkIsbUNBQVEsR0FBUjtRQUNFLE9BQU8sMEJBQXdCLElBQUksQ0FBQyxFQUFFLGdCQUFXLElBQUksQ0FBQyxHQUFHLCtCQUEwQixJQUFJLENBQUMsaUJBQWlCLGtCQUFhLElBQUksQ0FBQyxLQUFLLE1BQUcsQ0FBQztJQUN0SSxDQUFDO0lBQ0gsdUJBQUM7QUFBRCxDQUFDLEFBakJELENBQXNDLFdBQVcsR0FpQmhEOztBQUVEOzs7O0dBSUc7QUFDSDtJQUFzQyw0Q0FBVztJQUMvQztJQUNJLHVCQUF1QjtJQUN2QixFQUFVO0lBQ1YsdUJBQXVCO0lBQ3ZCLEdBQVc7SUFDWCx1QkFBdUI7SUFDaEIsaUJBQXlCO0lBQ2hDLHVCQUF1QjtJQUNoQixLQUEwQjtRQVJyQyxZQVNFLGtCQUFNLEVBQUUsRUFBRSxHQUFHLENBQUMsU0FDZjtRQUpVLHVCQUFpQixHQUFqQixpQkFBaUIsQ0FBUTtRQUV6QixXQUFLLEdBQUwsS0FBSyxDQUFxQjs7SUFFckMsQ0FBQztJQUVELG1DQUFRLEdBQVI7UUFDRSxPQUFPLDBCQUF3QixJQUFJLENBQUMsRUFBRSxnQkFBVyxJQUFJLENBQUMsR0FBRywrQkFBMEIsSUFBSSxDQUFDLGlCQUFpQixrQkFBYSxJQUFJLENBQUMsS0FBSyxNQUFHLENBQUM7SUFDdEksQ0FBQztJQUNILHVCQUFDO0FBQUQsQ0FBQyxBQWhCRCxDQUFzQyxXQUFXLEdBZ0JoRDs7QUFFRDs7OztHQUlHO0FBQ0g7SUFBb0MsMENBQVc7SUFDN0M7SUFDSSx1QkFBdUI7SUFDdkIsRUFBVTtJQUNWLHVCQUF1QjtJQUN2QixHQUFXO0lBQ1gsdUJBQXVCO0lBQ2hCLGlCQUF5QjtJQUNoQyx1QkFBdUI7SUFDaEIsS0FBMEI7SUFDakMsdUJBQXVCO0lBQ2hCLGNBQXVCO1FBVmxDLFlBV0Usa0JBQU0sRUFBRSxFQUFFLEdBQUcsQ0FBQyxTQUNmO1FBTlUsdUJBQWlCLEdBQWpCLGlCQUFpQixDQUFRO1FBRXpCLFdBQUssR0FBTCxLQUFLLENBQXFCO1FBRTFCLG9CQUFjLEdBQWQsY0FBYyxDQUFTOztJQUVsQyxDQUFDO0lBRUQsaUNBQVEsR0FBUjtRQUNFLE9BQU8sd0JBQXNCLElBQUksQ0FBQyxFQUFFLGdCQUFXLElBQUksQ0FBQyxHQUFHLCtCQUEwQixJQUFJLENBQUMsaUJBQWlCLGtCQUFhLElBQUksQ0FBQyxLQUFLLDBCQUFxQixJQUFJLENBQUMsY0FBYyxNQUFHLENBQUM7SUFDNUssQ0FBQztJQUNILHFCQUFDO0FBQUQsQ0FBQyxBQWxCRCxDQUFvQyxXQUFXLEdBa0I5Qzs7QUFFRDs7Ozs7OztHQU9HO0FBQ0g7SUFBa0Msd0NBQVc7SUFDM0M7SUFDSSx1QkFBdUI7SUFDdkIsRUFBVTtJQUNWLHVCQUF1QjtJQUN2QixHQUFXO0lBQ1gsdUJBQXVCO0lBQ2hCLGlCQUF5QjtJQUNoQyx1QkFBdUI7SUFDaEIsS0FBMEI7UUFSckMsWUFTRSxrQkFBTSxFQUFFLEVBQUUsR0FBRyxDQUFDLFNBQ2Y7UUFKVSx1QkFBaUIsR0FBakIsaUJBQWlCLENBQVE7UUFFekIsV0FBSyxHQUFMLEtBQUssQ0FBcUI7O0lBRXJDLENBQUM7SUFFRCwrQkFBUSxHQUFSO1FBQ0UsT0FBTyxzQkFBb0IsSUFBSSxDQUFDLEVBQUUsZ0JBQVcsSUFBSSxDQUFDLEdBQUcsK0JBQTBCLElBQUksQ0FBQyxpQkFBaUIsa0JBQWEsSUFBSSxDQUFDLEtBQUssTUFBRyxDQUFDO0lBQ2xJLENBQUM7SUFDSCxtQkFBQztBQUFELENBQUMsQUFoQkQsQ0FBa0MsV0FBVyxHQWdCNUM7O0FBRUQ7Ozs7O0dBS0c7QUFDSDtJQUFnQyxzQ0FBVztJQUN6QztJQUNJLHVCQUF1QjtJQUN2QixFQUFVO0lBQ1YsdUJBQXVCO0lBQ3ZCLEdBQVc7SUFDWCx1QkFBdUI7SUFDaEIsaUJBQXlCO0lBQ2hDLHVCQUF1QjtJQUNoQixLQUEwQjtRQVJyQyxZQVNFLGtCQUFNLEVBQUUsRUFBRSxHQUFHLENBQUMsU0FDZjtRQUpVLHVCQUFpQixHQUFqQixpQkFBaUIsQ0FBUTtRQUV6QixXQUFLLEdBQUwsS0FBSyxDQUFxQjs7SUFFckMsQ0FBQztJQUVELDZCQUFRLEdBQVI7UUFDRSxPQUFPLG9CQUFrQixJQUFJLENBQUMsRUFBRSxnQkFBVyxJQUFJLENBQUMsR0FBRywrQkFBMEIsSUFBSSxDQUFDLGlCQUFpQixrQkFBYSxJQUFJLENBQUMsS0FBSyxNQUFHLENBQUM7SUFDaEksQ0FBQztJQUNILGlCQUFDO0FBQUQsQ0FBQyxBQWhCRCxDQUFnQyxXQUFXLEdBZ0IxQzs7QUFFRDs7OztHQUlHO0FBQ0g7SUFDRTtJQUNJLHVCQUF1QjtJQUNoQixLQUFZO1FBQVosVUFBSyxHQUFMLEtBQUssQ0FBTztJQUFHLENBQUM7SUFDM0IsdUNBQVEsR0FBUixjQUFxQixPQUFPLGdDQUE4QixJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksTUFBRyxDQUFDLENBQUMsQ0FBQztJQUNqRiwyQkFBQztBQUFELENBQUMsQUFMRCxJQUtDOztBQUVEOzs7O0dBSUc7QUFDSDtJQUNFO0lBQ0ksdUJBQXVCO0lBQ2hCLEtBQVk7UUFBWixVQUFLLEdBQUwsS0FBSyxDQUFPO0lBQUcsQ0FBQztJQUMzQixxQ0FBUSxHQUFSLGNBQXFCLE9BQU8sOEJBQTRCLElBQUksQ0FBQyxLQUFLLENBQUMsSUFBSSxNQUFHLENBQUMsQ0FBQyxDQUFDO0lBQy9FLHlCQUFDO0FBQUQsQ0FBQyxBQUxELElBS0M7O0FBRUQ7Ozs7Ozs7R0FPRztBQUNIO0lBQ0U7SUFDSSx1QkFBdUI7SUFDaEIsUUFBZ0M7UUFBaEMsYUFBUSxHQUFSLFFBQVEsQ0FBd0I7SUFBRyxDQUFDO0lBQy9DLHVDQUFRLEdBQVI7UUFDRSxJQUFNLElBQUksR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQy9FLE9BQU8saUNBQStCLElBQUksT0FBSSxDQUFDO0lBQ2pELENBQUM7SUFDSCwyQkFBQztBQUFELENBQUMsQUFSRCxJQVFDOztBQUVEOzs7Ozs7R0FNRztBQUNIO0lBQ0U7SUFDSSx1QkFBdUI7SUFDaEIsUUFBZ0M7UUFBaEMsYUFBUSxHQUFSLFFBQVEsQ0FBd0I7SUFBRyxDQUFDO0lBQy9DLHFDQUFRLEdBQVI7UUFDRSxJQUFNLElBQUksR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQy9FLE9BQU8sK0JBQTZCLElBQUksT0FBSSxDQUFDO0lBQy9DLENBQUM7SUFDSCx5QkFBQztBQUFELENBQUMsQUFSRCxJQVFDOztBQUVEOzs7Ozs7O0dBT0c7QUFDSDtJQUNFO0lBQ0ksdUJBQXVCO0lBQ2hCLFFBQWdDO1FBQWhDLGFBQVEsR0FBUixRQUFRLENBQXdCO0lBQUcsQ0FBQztJQUMvQyxrQ0FBUSxHQUFSO1FBQ0UsSUFBTSxJQUFJLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxJQUFJLEVBQUUsQ0FBQztRQUMvRSxPQUFPLDRCQUEwQixJQUFJLE9BQUksQ0FBQztJQUM1QyxDQUFDO0lBQ0gsc0JBQUM7QUFBRCxDQUFDLEFBUkQsSUFRQzs7QUFFRDs7Ozs7OztHQU9HO0FBQ0g7SUFDRTtJQUNJLHVCQUF1QjtJQUNoQixRQUFnQztRQUFoQyxhQUFRLEdBQVIsUUFBUSxDQUF3QjtJQUFHLENBQUM7SUFDL0MsZ0NBQVEsR0FBUjtRQUNFLElBQU0sSUFBSSxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxDQUFDLElBQUksSUFBSSxFQUFFLENBQUM7UUFDL0UsT0FBTywwQkFBd0IsSUFBSSxPQUFJLENBQUM7SUFDMUMsQ0FBQztJQUNILG9CQUFDO0FBQUQsQ0FBQyxBQVJELElBUUM7O0FBRUQ7Ozs7R0FJRztBQUNIO0lBQ0U7SUFDSSx1QkFBdUI7SUFDZCxXQUEwQjtJQUVuQyx1QkFBdUI7SUFDZCxRQUErQjtJQUV4Qyx1QkFBdUI7SUFDZCxNQUFtQjtRQU5uQixnQkFBVyxHQUFYLFdBQVcsQ0FBZTtRQUcxQixhQUFRLEdBQVIsUUFBUSxDQUF1QjtRQUcvQixXQUFNLEdBQU4sTUFBTSxDQUFhO0lBQUcsQ0FBQztJQUVwQyx5QkFBUSxHQUFSO1FBQ0UsSUFBTSxHQUFHLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsVUFBSyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBRyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7UUFDOUUsT0FBTyxxQkFBbUIsSUFBSSxDQUFDLE1BQU0sc0JBQWlCLEdBQUcsT0FBSSxDQUFDO0lBQ2hFLENBQUM7SUFDSCxhQUFDO0FBQUQsQ0FBQyxBQWZELElBZUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Um91dGV9IGZyb20gJy4vY29uZmlnJztcbmltcG9ydCB7QWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgUm91dGVyU3RhdGVTbmFwc2hvdH0gZnJvbSAnLi9yb3V0ZXJfc3RhdGUnO1xuXG4vKipcbiAqIElkZW50aWZpZXMgdGhlIGNhbGwgb3IgZXZlbnQgdGhhdCB0cmlnZ2VyZWQgYSBuYXZpZ2F0aW9uLlxuICpcbiAqICogJ2ltcGVyYXRpdmUnOiBUcmlnZ2VyZWQgYnkgYHJvdXRlci5uYXZpZ2F0ZUJ5VXJsKClgIG9yIGByb3V0ZXIubmF2aWdhdGUoKWAuXG4gKiAqICdwb3BzdGF0ZScgOiBUcmlnZ2VyZWQgYnkgYSBgcG9wc3RhdGVgIGV2ZW50LlxuICogKiAnaGFzaGNoYW5nZSctOiBUcmlnZ2VyZWQgYnkgYSBgaGFzaGNoYW5nZWAgZXZlbnQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgdHlwZSBOYXZpZ2F0aW9uVHJpZ2dlciA9ICdpbXBlcmF0aXZlJyB8ICdwb3BzdGF0ZScgfCAnaGFzaGNoYW5nZSc7XG5cbi8qKlxuICogQmFzZSBmb3IgZXZlbnRzIHRoZSByb3V0ZXIgZ29lcyB0aHJvdWdoLCBhcyBvcHBvc2VkIHRvIGV2ZW50cyB0aWVkIHRvIGEgc3BlY2lmaWNcbiAqIHJvdXRlLiBGaXJlZCBvbmUgdGltZSBmb3IgYW55IGdpdmVuIG5hdmlnYXRpb24uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqXG4gKiBgYGB0c1xuICogY2xhc3MgTXlTZXJ2aWNlIHtcbiAqICAgY29uc3RydWN0b3IocHVibGljIHJvdXRlcjogUm91dGVyLCBsb2dnZXI6IExvZ2dlcikge1xuICogICAgIHJvdXRlci5ldmVudHMucGlwZShcbiAqICAgICAgIGZpbHRlcihlID0+IGUgaW5zdGFuY2VvZiBSb3V0ZXJFdmVudClcbiAqICAgICApLnN1YnNjcmliZShlID0+IHtcbiAqICAgICAgIGxvZ2dlci5sb2coZS5pZCwgZS51cmwpO1xuICogICAgIH0pO1xuICogICB9XG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAc2VlIGBFdmVudGBcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFJvdXRlckV2ZW50IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogQSB1bmlxdWUgSUQgdGhhdCB0aGUgcm91dGVyIGFzc2lnbnMgdG8gZXZlcnkgcm91dGVyIG5hdmlnYXRpb24uICovXG4gICAgICBwdWJsaWMgaWQ6IG51bWJlcixcbiAgICAgIC8qKiBUaGUgVVJMIHRoYXQgaXMgdGhlIGRlc3RpbmF0aW9uIGZvciB0aGlzIG5hdmlnYXRpb24uICovXG4gICAgICBwdWJsaWMgdXJsOiBzdHJpbmcpIHt9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIHdoZW4gYSBuYXZpZ2F0aW9uIHN0YXJ0cy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBOYXZpZ2F0aW9uU3RhcnQgZXh0ZW5kcyBSb3V0ZXJFdmVudCB7XG4gIC8qKlxuICAgKiBJZGVudGlmaWVzIHRoZSBjYWxsIG9yIGV2ZW50IHRoYXQgdHJpZ2dlcmVkIHRoZSBuYXZpZ2F0aW9uLlxuICAgKiBBbiBgaW1wZXJhdGl2ZWAgdHJpZ2dlciBpcyBhIGNhbGwgdG8gYHJvdXRlci5uYXZpZ2F0ZUJ5VXJsKClgIG9yIGByb3V0ZXIubmF2aWdhdGUoKWAuXG4gICAqXG4gICAqL1xuICBuYXZpZ2F0aW9uVHJpZ2dlcj86ICdpbXBlcmF0aXZlJ3wncG9wc3RhdGUnfCdoYXNoY2hhbmdlJztcblxuICAvKipcbiAgICogVGhlIG5hdmlnYXRpb24gc3RhdGUgdGhhdCB3YXMgcHJldmlvdXNseSBzdXBwbGllZCB0byB0aGUgYHB1c2hTdGF0ZWAgY2FsbCxcbiAgICogd2hlbiB0aGUgbmF2aWdhdGlvbiBpcyB0cmlnZ2VyZWQgYnkgYSBgcG9wc3RhdGVgIGV2ZW50LiBPdGhlcndpc2UgbnVsbC5cbiAgICpcbiAgICogVGhlIHN0YXRlIG9iamVjdCBpcyBkZWZpbmVkIGJ5IGBOYXZpZ2F0aW9uRXh0cmFzYCwgYW5kIGNvbnRhaW5zIGFueVxuICAgKiBkZXZlbG9wZXItZGVmaW5lZCBzdGF0ZSB2YWx1ZSwgYXMgd2VsbCBhcyBhIHVuaXF1ZSBJRCB0aGF0XG4gICAqIHRoZSByb3V0ZXIgYXNzaWducyB0byBldmVyeSByb3V0ZXIgdHJhbnNpdGlvbi9uYXZpZ2F0aW9uLlxuICAgKlxuICAgKiBGcm9tIHRoZSBwZXJzcGVjdGl2ZSBvZiB0aGUgcm91dGVyLCB0aGUgcm91dGVyIG5ldmVyIFwiZ29lcyBiYWNrXCIuXG4gICAqIFdoZW4gdGhlIHVzZXIgY2xpY2tzIG9uIHRoZSBiYWNrIGJ1dHRvbiBpbiB0aGUgYnJvd3NlcixcbiAgICogYSBuZXcgbmF2aWdhdGlvbiBJRCBpcyBjcmVhdGVkLlxuICAgKlxuICAgKiBVc2UgdGhlIElEIGluIHRoaXMgcHJldmlvdXMtc3RhdGUgb2JqZWN0IHRvIGRpZmZlcmVudGlhdGUgYmV0d2VlbiBhIG5ld2x5IGNyZWF0ZWRcbiAgICogc3RhdGUgYW5kIG9uZSByZXR1cm5lZCB0byBieSBhIGBwb3BzdGF0ZWAgZXZlbnQsIHNvIHRoYXQgeW91IGNhbiByZXN0b3JlIHNvbWVcbiAgICogcmVtZW1iZXJlZCBzdGF0ZSwgc3VjaCBhcyBzY3JvbGwgcG9zaXRpb24uXG4gICAqXG4gICAqL1xuICByZXN0b3JlZFN0YXRlPzoge1trOiBzdHJpbmddOiBhbnksIG5hdmlnYXRpb25JZDogbnVtYmVyfXxudWxsO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIGlkOiBudW1iZXIsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgdXJsOiBzdHJpbmcsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgbmF2aWdhdGlvblRyaWdnZXI6ICdpbXBlcmF0aXZlJ3wncG9wc3RhdGUnfCdoYXNoY2hhbmdlJyA9ICdpbXBlcmF0aXZlJyxcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICByZXN0b3JlZFN0YXRlOiB7W2s6IHN0cmluZ106IGFueSwgbmF2aWdhdGlvbklkOiBudW1iZXJ9fG51bGwgPSBudWxsKSB7XG4gICAgc3VwZXIoaWQsIHVybCk7XG4gICAgdGhpcy5uYXZpZ2F0aW9uVHJpZ2dlciA9IG5hdmlnYXRpb25UcmlnZ2VyO1xuICAgIHRoaXMucmVzdG9yZWRTdGF0ZSA9IHJlc3RvcmVkU3RhdGU7XG4gIH1cblxuICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICB0b1N0cmluZygpOiBzdHJpbmcgeyByZXR1cm4gYE5hdmlnYXRpb25TdGFydChpZDogJHt0aGlzLmlkfSwgdXJsOiAnJHt0aGlzLnVybH0nKWA7IH1cbn1cblxuLyoqXG4gKiBBbiBldmVudCB0cmlnZ2VyZWQgd2hlbiBhIG5hdmlnYXRpb24gZW5kcyBzdWNjZXNzZnVsbHkuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgTmF2aWdhdGlvbkVuZCBleHRlbmRzIFJvdXRlckV2ZW50IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgaWQ6IG51bWJlcixcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICB1cmw6IHN0cmluZyxcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgdXJsQWZ0ZXJSZWRpcmVjdHM6IHN0cmluZykge1xuICAgIHN1cGVyKGlkLCB1cmwpO1xuICB9XG5cbiAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gYE5hdmlnYXRpb25FbmQoaWQ6ICR7dGhpcy5pZH0sIHVybDogJyR7dGhpcy51cmx9JywgdXJsQWZ0ZXJSZWRpcmVjdHM6ICcke3RoaXMudXJsQWZ0ZXJSZWRpcmVjdHN9JylgO1xuICB9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIHdoZW4gYSBuYXZpZ2F0aW9uIGlzIGNhbmNlbGVkLCBkaXJlY3RseSBvciBpbmRpcmVjdGx5LlxuICpcbiAqIFRoaXMgY2FuIGhhcHBlbiB3aGVuIGEgW3JvdXRlIGd1YXJkXShndWlkZS9yb3V0ZXIjbWlsZXN0b25lLTUtcm91dGUtZ3VhcmRzKVxuICogcmV0dXJucyBgZmFsc2VgIG9yIGluaXRpYXRlcyBhIHJlZGlyZWN0IGJ5IHJldHVybmluZyBhIGBVcmxUcmVlYC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBOYXZpZ2F0aW9uQ2FuY2VsIGV4dGVuZHMgUm91dGVyRXZlbnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBpZDogbnVtYmVyLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHVybDogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyByZWFzb246IHN0cmluZykge1xuICAgIHN1cGVyKGlkLCB1cmwpO1xuICB9XG5cbiAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHsgcmV0dXJuIGBOYXZpZ2F0aW9uQ2FuY2VsKGlkOiAke3RoaXMuaWR9LCB1cmw6ICcke3RoaXMudXJsfScpYDsgfVxufVxuXG4vKipcbiAqIEFuIGV2ZW50IHRyaWdnZXJlZCB3aGVuIGEgbmF2aWdhdGlvbiBmYWlscyBkdWUgdG8gYW4gdW5leHBlY3RlZCBlcnJvci5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBOYXZpZ2F0aW9uRXJyb3IgZXh0ZW5kcyBSb3V0ZXJFdmVudCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIGlkOiBudW1iZXIsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgdXJsOiBzdHJpbmcsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgcHVibGljIGVycm9yOiBhbnkpIHtcbiAgICBzdXBlcihpZCwgdXJsKTtcbiAgfVxuXG4gIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGBOYXZpZ2F0aW9uRXJyb3IoaWQ6ICR7dGhpcy5pZH0sIHVybDogJyR7dGhpcy51cmx9JywgZXJyb3I6ICR7dGhpcy5lcnJvcn0pYDtcbiAgfVxufVxuXG4vKipcbiAqQW4gZXZlbnQgdHJpZ2dlcmVkIHdoZW4gcm91dGVzIGFyZSByZWNvZ25pemVkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFJvdXRlc1JlY29nbml6ZWQgZXh0ZW5kcyBSb3V0ZXJFdmVudCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIGlkOiBudW1iZXIsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgdXJsOiBzdHJpbmcsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgcHVibGljIHVybEFmdGVyUmVkaXJlY3RzOiBzdHJpbmcsXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgcHVibGljIHN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90KSB7XG4gICAgc3VwZXIoaWQsIHVybCk7XG4gIH1cblxuICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICB0b1N0cmluZygpOiBzdHJpbmcge1xuICAgIHJldHVybiBgUm91dGVzUmVjb2duaXplZChpZDogJHt0aGlzLmlkfSwgdXJsOiAnJHt0aGlzLnVybH0nLCB1cmxBZnRlclJlZGlyZWN0czogJyR7dGhpcy51cmxBZnRlclJlZGlyZWN0c30nLCBzdGF0ZTogJHt0aGlzLnN0YXRlfSlgO1xuICB9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIGF0IHRoZSBzdGFydCBvZiB0aGUgR3VhcmQgcGhhc2Ugb2Ygcm91dGluZy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBHdWFyZHNDaGVja1N0YXJ0IGV4dGVuZHMgUm91dGVyRXZlbnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBpZDogbnVtYmVyLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHVybDogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyB1cmxBZnRlclJlZGlyZWN0czogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyBzdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCkge1xuICAgIHN1cGVyKGlkLCB1cmwpO1xuICB9XG5cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gYEd1YXJkc0NoZWNrU3RhcnQoaWQ6ICR7dGhpcy5pZH0sIHVybDogJyR7dGhpcy51cmx9JywgdXJsQWZ0ZXJSZWRpcmVjdHM6ICcke3RoaXMudXJsQWZ0ZXJSZWRpcmVjdHN9Jywgc3RhdGU6ICR7dGhpcy5zdGF0ZX0pYDtcbiAgfVxufVxuXG4vKipcbiAqIEFuIGV2ZW50IHRyaWdnZXJlZCBhdCB0aGUgZW5kIG9mIHRoZSBHdWFyZCBwaGFzZSBvZiByb3V0aW5nLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIEd1YXJkc0NoZWNrRW5kIGV4dGVuZHMgUm91dGVyRXZlbnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBpZDogbnVtYmVyLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHVybDogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyB1cmxBZnRlclJlZGlyZWN0czogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyBzdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCxcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgc2hvdWxkQWN0aXZhdGU6IGJvb2xlYW4pIHtcbiAgICBzdXBlcihpZCwgdXJsKTtcbiAgfVxuXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGBHdWFyZHNDaGVja0VuZChpZDogJHt0aGlzLmlkfSwgdXJsOiAnJHt0aGlzLnVybH0nLCB1cmxBZnRlclJlZGlyZWN0czogJyR7dGhpcy51cmxBZnRlclJlZGlyZWN0c30nLCBzdGF0ZTogJHt0aGlzLnN0YXRlfSwgc2hvdWxkQWN0aXZhdGU6ICR7dGhpcy5zaG91bGRBY3RpdmF0ZX0pYDtcbiAgfVxufVxuXG4vKipcbiAqIEFuIGV2ZW50IHRyaWdnZXJlZCBhdCB0aGUgdGhlIHN0YXJ0IG9mIHRoZSBSZXNvbHZlIHBoYXNlIG9mIHJvdXRpbmcuXG4gKlxuICogUnVucyBpbiB0aGUgXCJyZXNvbHZlXCIgcGhhc2Ugd2hldGhlciBvciBub3QgdGhlcmUgaXMgYW55dGhpbmcgdG8gcmVzb2x2ZS5cbiAqIEluIGZ1dHVyZSwgbWF5IGNoYW5nZSB0byBvbmx5IHJ1biB3aGVuIHRoZXJlIGFyZSB0aGluZ3MgdG8gYmUgcmVzb2x2ZWQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgUmVzb2x2ZVN0YXJ0IGV4dGVuZHMgUm91dGVyRXZlbnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBpZDogbnVtYmVyLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHVybDogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyB1cmxBZnRlclJlZGlyZWN0czogc3RyaW5nLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyBzdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCkge1xuICAgIHN1cGVyKGlkLCB1cmwpO1xuICB9XG5cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gYFJlc29sdmVTdGFydChpZDogJHt0aGlzLmlkfSwgdXJsOiAnJHt0aGlzLnVybH0nLCB1cmxBZnRlclJlZGlyZWN0czogJyR7dGhpcy51cmxBZnRlclJlZGlyZWN0c30nLCBzdGF0ZTogJHt0aGlzLnN0YXRlfSlgO1xuICB9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIGF0IHRoZSBlbmQgb2YgdGhlIFJlc29sdmUgcGhhc2Ugb2Ygcm91dGluZy5cbiAqIEBzZWUgYFJlc29sdmVTdGFydGAuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgUmVzb2x2ZUVuZCBleHRlbmRzIFJvdXRlckV2ZW50IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgaWQ6IG51bWJlcixcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICB1cmw6IHN0cmluZyxcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgdXJsQWZ0ZXJSZWRpcmVjdHM6IHN0cmluZyxcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpIHtcbiAgICBzdXBlcihpZCwgdXJsKTtcbiAgfVxuXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGBSZXNvbHZlRW5kKGlkOiAke3RoaXMuaWR9LCB1cmw6ICcke3RoaXMudXJsfScsIHVybEFmdGVyUmVkaXJlY3RzOiAnJHt0aGlzLnVybEFmdGVyUmVkaXJlY3RzfScsIHN0YXRlOiAke3RoaXMuc3RhdGV9KWA7XG4gIH1cbn1cblxuLyoqXG4gKiBBbiBldmVudCB0cmlnZ2VyZWQgYmVmb3JlIGxhenkgbG9hZGluZyBhIHJvdXRlIGNvbmZpZ3VyYXRpb24uXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgUm91dGVDb25maWdMb2FkU3RhcnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgcm91dGU6IFJvdXRlKSB7fVxuICB0b1N0cmluZygpOiBzdHJpbmcgeyByZXR1cm4gYFJvdXRlQ29uZmlnTG9hZFN0YXJ0KHBhdGg6ICR7dGhpcy5yb3V0ZS5wYXRofSlgOyB9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIHdoZW4gYSByb3V0ZSBoYXMgYmVlbiBsYXp5IGxvYWRlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBSb3V0ZUNvbmZpZ0xvYWRFbmQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgcm91dGU6IFJvdXRlKSB7fVxuICB0b1N0cmluZygpOiBzdHJpbmcgeyByZXR1cm4gYFJvdXRlQ29uZmlnTG9hZEVuZChwYXRoOiAke3RoaXMucm91dGUucGF0aH0pYDsgfVxufVxuXG4vKipcbiAqIEFuIGV2ZW50IHRyaWdnZXJlZCBhdCB0aGUgc3RhcnQgb2YgdGhlIGNoaWxkLWFjdGl2YXRpb25cbiAqIHBhcnQgb2YgdGhlIFJlc29sdmUgcGhhc2Ugb2Ygcm91dGluZy5cbiAqIEBzZWUgIGBDaGlsZEFjdGl2YXRpb25FbmRgXG4gKiBAc2VlIGBSZXNvbHZlU3RhcnRgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQ2hpbGRBY3RpdmF0aW9uU3RhcnQge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICBwdWJsaWMgc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpIHt9XG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgY29uc3QgcGF0aCA9IHRoaXMuc25hcHNob3Qucm91dGVDb25maWcgJiYgdGhpcy5zbmFwc2hvdC5yb3V0ZUNvbmZpZy5wYXRoIHx8ICcnO1xuICAgIHJldHVybiBgQ2hpbGRBY3RpdmF0aW9uU3RhcnQocGF0aDogJyR7cGF0aH0nKWA7XG4gIH1cbn1cblxuLyoqXG4gKiBBbiBldmVudCB0cmlnZ2VyZWQgYXQgdGhlIGVuZCBvZiB0aGUgY2hpbGQtYWN0aXZhdGlvbiBwYXJ0XG4gKiBvZiB0aGUgUmVzb2x2ZSBwaGFzZSBvZiByb3V0aW5nLlxuICogQHNlZSBgQ2hpbGRBY3RpdmF0aW9uU3RhcnRgXG4gKiBAc2VlIGBSZXNvbHZlU3RhcnRgICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIENoaWxkQWN0aXZhdGlvbkVuZCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyBzbmFwc2hvdDogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCkge31cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICBjb25zdCBwYXRoID0gdGhpcy5zbmFwc2hvdC5yb3V0ZUNvbmZpZyAmJiB0aGlzLnNuYXBzaG90LnJvdXRlQ29uZmlnLnBhdGggfHwgJyc7XG4gICAgcmV0dXJuIGBDaGlsZEFjdGl2YXRpb25FbmQocGF0aDogJyR7cGF0aH0nKWA7XG4gIH1cbn1cblxuLyoqXG4gKiBBbiBldmVudCB0cmlnZ2VyZWQgYXQgdGhlIHN0YXJ0IG9mIHRoZSBhY3RpdmF0aW9uIHBhcnRcbiAqIG9mIHRoZSBSZXNvbHZlIHBoYXNlIG9mIHJvdXRpbmcuXG4gKiBAc2VlIEFjdGl2YXRpb25FbmRgXG4gKiBAc2VlIGBSZXNvbHZlU3RhcnRgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQWN0aXZhdGlvblN0YXJ0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICAgICAgcHVibGljIHNuYXBzaG90OiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KSB7fVxuICB0b1N0cmluZygpOiBzdHJpbmcge1xuICAgIGNvbnN0IHBhdGggPSB0aGlzLnNuYXBzaG90LnJvdXRlQ29uZmlnICYmIHRoaXMuc25hcHNob3Qucm91dGVDb25maWcucGF0aCB8fCAnJztcbiAgICByZXR1cm4gYEFjdGl2YXRpb25TdGFydChwYXRoOiAnJHtwYXRofScpYDtcbiAgfVxufVxuXG4vKipcbiAqIEFuIGV2ZW50IHRyaWdnZXJlZCBhdCB0aGUgZW5kIG9mIHRoZSBhY3RpdmF0aW9uIHBhcnRcbiAqIG9mIHRoZSBSZXNvbHZlIHBoYXNlIG9mIHJvdXRpbmcuXG4gKiBAc2VlIGBBY3RpdmF0aW9uU3RhcnRgXG4gKiBAc2VlIGBSZXNvbHZlU3RhcnRgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQWN0aXZhdGlvbkVuZCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHB1YmxpYyBzbmFwc2hvdDogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCkge31cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICBjb25zdCBwYXRoID0gdGhpcy5zbmFwc2hvdC5yb3V0ZUNvbmZpZyAmJiB0aGlzLnNuYXBzaG90LnJvdXRlQ29uZmlnLnBhdGggfHwgJyc7XG4gICAgcmV0dXJuIGBBY3RpdmF0aW9uRW5kKHBhdGg6ICcke3BhdGh9JylgO1xuICB9XG59XG5cbi8qKlxuICogQW4gZXZlbnQgdHJpZ2dlcmVkIGJ5IHNjcm9sbGluZy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBTY3JvbGwge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICByZWFkb25seSByb3V0ZXJFdmVudDogTmF2aWdhdGlvbkVuZCxcblxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgICAgIHJlYWRvbmx5IHBvc2l0aW9uOiBbbnVtYmVyLCBudW1iZXJdfG51bGwsXG5cbiAgICAgIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gICAgICByZWFkb25seSBhbmNob3I6IHN0cmluZ3xudWxsKSB7fVxuXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgY29uc3QgcG9zID0gdGhpcy5wb3NpdGlvbiA/IGAke3RoaXMucG9zaXRpb25bMF19LCAke3RoaXMucG9zaXRpb25bMV19YCA6IG51bGw7XG4gICAgcmV0dXJuIGBTY3JvbGwoYW5jaG9yOiAnJHt0aGlzLmFuY2hvcn0nLCBwb3NpdGlvbjogJyR7cG9zfScpYDtcbiAgfVxufVxuXG4vKipcbiAqIFJvdXRlciBldmVudHMgdGhhdCBhbGxvdyB5b3UgdG8gdHJhY2sgdGhlIGxpZmVjeWNsZSBvZiB0aGUgcm91dGVyLlxuICpcbiAqIFRoZSBzZXF1ZW5jZSBvZiByb3V0ZXIgZXZlbnRzIGlzIGFzIGZvbGxvd3M6XG4gKlxuICogLSBgTmF2aWdhdGlvblN0YXJ0YCxcbiAqIC0gYFJvdXRlQ29uZmlnTG9hZFN0YXJ0YCxcbiAqIC0gYFJvdXRlQ29uZmlnTG9hZEVuZGAsXG4gKiAtIGBSb3V0ZXNSZWNvZ25pemVkYCxcbiAqIC0gYEd1YXJkc0NoZWNrU3RhcnRgLFxuICogLSBgQ2hpbGRBY3RpdmF0aW9uU3RhcnRgLFxuICogLSBgQWN0aXZhdGlvblN0YXJ0YCxcbiAqIC0gYEd1YXJkc0NoZWNrRW5kYCxcbiAqIC0gYFJlc29sdmVTdGFydGAsXG4gKiAtIGBSZXNvbHZlRW5kYCxcbiAqIC0gYEFjdGl2YXRpb25FbmRgXG4gKiAtIGBDaGlsZEFjdGl2YXRpb25FbmRgXG4gKiAtIGBOYXZpZ2F0aW9uRW5kYCxcbiAqIC0gYE5hdmlnYXRpb25DYW5jZWxgLFxuICogLSBgTmF2aWdhdGlvbkVycm9yYFxuICogLSBgU2Nyb2xsYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IHR5cGUgRXZlbnQgPSBSb3V0ZXJFdmVudCB8IFJvdXRlQ29uZmlnTG9hZFN0YXJ0IHwgUm91dGVDb25maWdMb2FkRW5kIHwgQ2hpbGRBY3RpdmF0aW9uU3RhcnQgfFxuICAgIENoaWxkQWN0aXZhdGlvbkVuZCB8IEFjdGl2YXRpb25TdGFydCB8IEFjdGl2YXRpb25FbmQgfCBTY3JvbGw7XG4iXX0=