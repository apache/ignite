/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { NavigationEnd, NavigationStart, Scroll } from './events';
var RouterScroller = /** @class */ (function () {
    function RouterScroller(router, 
    /** @docsNotRequired */ viewportScroller, options) {
        if (options === void 0) { options = {}; }
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
    RouterScroller.prototype.init = function () {
        // we want to disable the automatic scrolling because having two places
        // responsible for scrolling results race conditions, especially given
        // that browser don't implement this behavior consistently
        if (this.options.scrollPositionRestoration !== 'disabled') {
            this.viewportScroller.setHistoryScrollRestoration('manual');
        }
        this.routerEventsSubscription = this.createScrollEvents();
        this.scrollEventsSubscription = this.consumeScrollEvents();
    };
    RouterScroller.prototype.createScrollEvents = function () {
        var _this = this;
        return this.router.events.subscribe(function (e) {
            if (e instanceof NavigationStart) {
                // store the scroll position of the current stable navigations.
                _this.store[_this.lastId] = _this.viewportScroller.getScrollPosition();
                _this.lastSource = e.navigationTrigger;
                _this.restoredId = e.restoredState ? e.restoredState.navigationId : 0;
            }
            else if (e instanceof NavigationEnd) {
                _this.lastId = e.id;
                _this.scheduleScrollEvent(e, _this.router.parseUrl(e.urlAfterRedirects).fragment);
            }
        });
    };
    RouterScroller.prototype.consumeScrollEvents = function () {
        var _this = this;
        return this.router.events.subscribe(function (e) {
            if (!(e instanceof Scroll))
                return;
            // a popstate event. The pop state event will always ignore anchor scrolling.
            if (e.position) {
                if (_this.options.scrollPositionRestoration === 'top') {
                    _this.viewportScroller.scrollToPosition([0, 0]);
                }
                else if (_this.options.scrollPositionRestoration === 'enabled') {
                    _this.viewportScroller.scrollToPosition(e.position);
                }
                // imperative navigation "forward"
            }
            else {
                if (e.anchor && _this.options.anchorScrolling === 'enabled') {
                    _this.viewportScroller.scrollToAnchor(e.anchor);
                }
                else if (_this.options.scrollPositionRestoration !== 'disabled') {
                    _this.viewportScroller.scrollToPosition([0, 0]);
                }
            }
        });
    };
    RouterScroller.prototype.scheduleScrollEvent = function (routerEvent, anchor) {
        this.router.triggerEvent(new Scroll(routerEvent, this.lastSource === 'popstate' ? this.store[this.restoredId] : null, anchor));
    };
    RouterScroller.prototype.ngOnDestroy = function () {
        if (this.routerEventsSubscription) {
            this.routerEventsSubscription.unsubscribe();
        }
        if (this.scrollEventsSubscription) {
            this.scrollEventsSubscription.unsubscribe();
        }
    };
    return RouterScroller;
}());
export { RouterScroller };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVyX3Njcm9sbGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9yb3V0ZXJfc2Nyb2xsZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBTUgsT0FBTyxFQUFDLGFBQWEsRUFBRSxlQUFlLEVBQUUsTUFBTSxFQUFDLE1BQU0sVUFBVSxDQUFDO0FBR2hFO0lBV0Usd0JBQ1ksTUFBYztJQUN0Qix1QkFBdUIsQ0FBaUIsZ0JBQWtDLEVBQVUsT0FHOUU7UUFIOEUsd0JBQUEsRUFBQSxZQUc5RTtRQUpFLFdBQU0sR0FBTixNQUFNLENBQVE7UUFDa0IscUJBQWdCLEdBQWhCLGdCQUFnQixDQUFrQjtRQUFVLFlBQU8sR0FBUCxPQUFPLENBR3JGO1FBVkYsV0FBTSxHQUFHLENBQUMsQ0FBQztRQUNYLGVBQVUsR0FBbUQsWUFBWSxDQUFDO1FBQzFFLGVBQVUsR0FBRyxDQUFDLENBQUM7UUFDZixVQUFLLEdBQXNDLEVBQUUsQ0FBQztRQVFwRCxxQ0FBcUM7UUFDckMsT0FBTyxDQUFDLHlCQUF5QixHQUFHLE9BQU8sQ0FBQyx5QkFBeUIsSUFBSSxVQUFVLENBQUM7UUFDcEYsT0FBTyxDQUFDLGVBQWUsR0FBRyxPQUFPLENBQUMsZUFBZSxJQUFJLFVBQVUsQ0FBQztJQUNsRSxDQUFDO0lBRUQsNkJBQUksR0FBSjtRQUNFLHVFQUF1RTtRQUN2RSxzRUFBc0U7UUFDdEUsMERBQTBEO1FBQzFELElBQUksSUFBSSxDQUFDLE9BQU8sQ0FBQyx5QkFBeUIsS0FBSyxVQUFVLEVBQUU7WUFDekQsSUFBSSxDQUFDLGdCQUFnQixDQUFDLDJCQUEyQixDQUFDLFFBQVEsQ0FBQyxDQUFDO1NBQzdEO1FBQ0QsSUFBSSxDQUFDLHdCQUF3QixHQUFHLElBQUksQ0FBQyxrQkFBa0IsRUFBRSxDQUFDO1FBQzFELElBQUksQ0FBQyx3QkFBd0IsR0FBRyxJQUFJLENB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Q2pDLElBQUksQ0FBQyx3QkFBd0IsQ0FBQyxXQUFXLEVBQUUsQ0FBQztTQUM3QztJQUNILENBQUM7SUFDSCxxQkFBQztBQUFELENBQUMsQUFqRkQsSUFpRkMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Vmlld3BvcnRTY3JvbGxlcn0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7T25EZXN0cm95fSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7VW5zdWJzY3JpYmFibGV9IGZyb20gJ3J4anMnO1xuXG5pbXBvcnQge05hdmlnYXRpb25FbmQsIE5hdmlnYXRpb25TdGFydCwgU2Nyb2xsfSBmcm9tICcuL2V2ZW50cyc7XG5pbXBvcnQge1JvdXRlcn0gZnJvbSAnLi9yb3V0ZXInO1xuXG5leHBvcnQgY2xhc3MgUm91dGVyU2Nyb2xsZXIgaW1wbGVtZW50cyBPbkRlc3Ryb3kge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSByb3V0ZXJFdmVudHNTdWJzY3JpcHRpb24gITogVW5zdWJzY3JpYmFibGU7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIHNjcm9sbEV2ZW50c1N1YnNjcmlwdGlvbiAhOiBVbnN1YnNjcmliYWJsZTtcblxuICBwcml2YXRlIGxhc3RJZCA9IDA7XG4gIHByaXZhdGUgbGFzdFNvdXJjZTogJ2ltcGVyYXRpdmUnfCdwb3BzdGF0ZSd8J2hhc2hjaGFuZ2UnfHVuZGVmaW5lZCA9ICdpbXBlcmF0aXZlJztcbiAgcHJpdmF0ZSByZXN0b3JlZElkID0gMDtcbiAgcHJpdmF0ZSBzdG9yZToge1trZXk6IHN0cmluZ106IFtudW1iZXIsIG51bWJlcl19ID0ge307XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIHJvdXRlcjogUm91dGVyLFxuICAgICAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi8gcHVibGljIHJlYWRvbmx5IHZpZXdwb3J0U2Nyb2xsZXI6IFZpZXdwb3J0U2Nyb2xsZXIsIHByaXZhdGUgb3B0aW9uczoge1xuICAgICAgICBzY3JvbGxQb3NpdGlvblJlc3RvcmF0aW9uPzogJ2Rpc2FibGVkJyB8ICdlbmFibGVkJyB8ICd0b3AnLFxuICAgICAgICBhbmNob3JTY3JvbGxpbmc/OiAnZGlzYWJsZWQnfCdlbmFibGVkJ1xuICAgICAgfSA9IHt9KSB7XG4gICAgLy8gRGVmYXVsdCBib3RoIG9wdGlvbnMgdG8gJ2Rpc2FibGVkJ1xuICAgIG9wdGlvbnMuc2Nyb2xsUG9zaXRpb25SZXN0b3JhdGlvbiA9IG9wdGlvbnMuc2Nyb2xsUG9zaXRpb25SZXN0b3JhdGlvbiB8fCAnZGlzYWJsZWQnO1xuICAgIG9wdGlvbnMuYW5jaG9yU2Nyb2xsaW5nID0gb3B0aW9ucy5hbmNob3JTY3JvbGxpbmcgfHwgJ2Rpc2FibGVkJztcbiAgfVxuXG4gIGluaXQoKTogdm9pZCB7XG4gICAgLy8gd2Ugd2FudCB0byBkaXNhYmxlIHRoZSBhdXRvbWF0aWMgc2Nyb2xsaW5nIGJlY2F1c2UgaGF2aW5nIHR3byBwbGFjZXNcbiAgICAvLyByZXNwb25zaWJsZSBmb3Igc2Nyb2xsaW5nIHJlc3VsdHMgcmFjZSBjb25kaXRpb25zLCBlc3BlY2lhbGx5IGdpdmVuXG4gICAgLy8gdGhhdCBicm93c2VyIGRvbid0IGltcGxlbWVudCB0aGlzIGJlaGF2aW9yIGNvbnNpc3RlbnRseVxuICAgIGlmICh0aGlzLm9wdGlvbnMuc2Nyb2xsUG9zaXRpb25SZXN0b3JhdGlvbiAhPT0gJ2Rpc2FibGVkJykge1xuICAgICAgdGhpcy52aWV3cG9ydFNjcm9sbGVyLnNldEhpc3RvcnlTY3JvbGxSZXN0b3JhdGlvbignbWFudWFsJyk7XG4gICAgfVxuICAgIHRoaXMucm91dGVyRXZlbnRzU3Vic2NyaXB0aW9uID0gdGhpcy5jcmVhdGVTY3JvbGxFdmVudHMoKTtcbiAgICB0aGlzLnNjcm9sbEV2ZW50c1N1YnNjcmlwdGlvbiA9IHRoaXMuY29uc3VtZVNjcm9sbEV2ZW50cygpO1xuICB9XG5cbiAgcHJpdmF0ZSBjcmVhdGVTY3JvbGxFdmVudHMoKSB7XG4gICAgcmV0dXJuIHRoaXMucm91dGVyLmV2ZW50cy5zdWJzY3JpYmUoZSA9PiB7XG4gICAgICBpZiAoZSBpbnN0YW5jZW9mIE5hdmlnYXRpb25TdGFydCkge1xuICAgICAgICAvLyBzdG9yZSB0aGUgc2Nyb2xsIHBvc2l0aW9uIG9mIHRoZSBjdXJyZW50IHN0YWJsZSBuYXZpZ2F0aW9ucy5cbiAgICAgICAgdGhpcy5zdG9yZVt0aGlzLmxhc3RJZF0gPSB0aGlzLnZpZXdwb3J0U2Nyb2xsZXIuZ2V0U2Nyb2xsUG9zaXRpb24oKTtcbiAgICAgICAgdGhpcy5sYXN0U291cmNlID0gZS5uYXZpZ2F0aW9uVHJpZ2dlcjtcbiAgICAgICAgdGhpcy5yZXN0b3JlZElkID0gZS5yZXN0b3JlZFN0YXRlID8gZS5yZXN0b3JlZFN0YXRlLm5hdmlnYXRpb25JZCA6IDA7XG4gICAgICB9IGVsc2UgaWYgKGUgaW5zdGFuY2VvZiBOYXZpZ2F0aW9uRW5kKSB7XG4gICAgICAgIHRoaXMubGFzdElkID0gZS5pZDtcbiAgICAgICAgdGhpcy5zY2hlZHVsZVNjcm9sbEV2ZW50KGUsIHRoaXMucm91dGVyLnBhcnNlVXJsKGUudXJsQWZ0ZXJSZWRpcmVjdHMpLmZyYWdtZW50KTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgY29uc3VtZVNjcm9sbEV2ZW50cygpIHtcbiAgICByZXR1cm4gdGhpcy5yb3V0ZXIuZXZlbnRzLnN1YnNjcmliZShlID0+IHtcbiAgICAgIGlmICghKGUgaW5zdGFuY2VvZiBTY3JvbGwpKSByZXR1cm47XG4gICAgICAvLyBhIHBvcHN0YXRlIGV2ZW50LiBUaGUgcG9wIHN0YXRlIGV2ZW50IHdpbGwgYWx3YXlzIGlnbm9yZSBhbmNob3Igc2Nyb2xsaW5nLlxuICAgICAgaWYgKGUucG9zaXRpb24pIHtcbiAgICAgICAgaWYgKHRoaXMub3B0aW9ucy5zY3JvbGxQb3NpdGlvblJlc3RvcmF0aW9uID09PSAndG9wJykge1xuICAgICAgICAgIHRoaXMudmlld3BvcnRTY3JvbGxlci5zY3JvbGxUb1Bvc2l0aW9uKFswLCAwXSk7XG4gICAgICAgIH0gZWxzZSBpZiAodGhpcy5vcHRpb25zLnNjcm9sbFBvc2l0aW9uUmVzdG9yYXRpb24gPT09ICdlbmFibGVkJykge1xuICAgICAgICAgIHRoaXMudmlld3BvcnRTY3JvbGxlci5zY3JvbGxUb1Bvc2l0aW9uKGUucG9zaXRpb24pO1xuICAgICAgICB9XG4gICAgICAgIC8vIGltcGVyYXRpdmUgbmF2aWdhdGlvbiBcImZvcndhcmRcIlxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaWYgKGUuYW5jaG9yICYmIHRoaXMub3B0aW9ucy5hbmNob3JTY3JvbGxpbmcgPT09ICdlbmFibGVkJykge1xuICAgICAgICAgIHRoaXMudmlld3BvcnRTY3JvbGxlci5zY3JvbGxUb0FuY2hvcihlLmFuY2hvcik7XG4gICAgICAgIH0gZWxzZSBpZiAodGhpcy5vcHRpb25zLnNjcm9sbFBvc2l0aW9uUmVzdG9yYXRpb24gIT09ICdkaXNhYmxlZCcpIHtcbiAgICAgICAgICB0aGlzLnZpZXdwb3J0U2Nyb2xsZXIuc2Nyb2xsVG9Qb3NpdGlvbihbMCwgMF0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIHNjaGVkdWxlU2Nyb2xsRXZlbnQocm91dGVyRXZlbnQ6IE5hdmlnYXRpb25FbmQsIGFuY2hvcjogc3RyaW5nfG51bGwpOiB2b2lkIHtcbiAgICB0aGlzLnJvdXRlci50cmlnZ2VyRXZlbnQobmV3IFNjcm9sbChcbiAgICAgICAgcm91dGVyRXZlbnQsIHRoaXMubGFzdFNvdXJjZSA9PT0gJ3BvcHN0YXRlJyA/IHRoaXMuc3RvcmVbdGhpcy5yZXN0b3JlZElkXSA6IG51bGwsIGFuY2hvcikpO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKSB7XG4gICAgaWYgKHRoaXMucm91dGVyRXZlbnRzU3Vic2NyaXB0aW9uKSB7XG4gICAgICB0aGlzLnJvdXRlckV2ZW50c1N1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICAgIH1cbiAgICBpZiAodGhpcy5zY3JvbGxFdmVudHNTdWJzY3JpcHRpb24pIHtcbiAgICAgIHRoaXMuc2Nyb2xsRXZlbnRzU3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgfVxuICB9XG59XG4iXX0=