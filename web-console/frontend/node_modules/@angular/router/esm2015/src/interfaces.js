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
 * Interface that a class can implement to be a guard deciding if a route can be activated.
 * If all guards return `true`, navigation will continue. If any guard returns `false`,
 * navigation will be cancelled. If any guard returns a `UrlTree`, current navigation will
 * be cancelled and a new navigation will be kicked off to the `UrlTree` returned from the
 * guard.
 *
 * ```
 * class UserToken {}
 * class Permissions {
 *   canActivate(user: UserToken, id: string): boolean {
 *     return true;
 *   }
 * }
 *
 * \@Injectable()
 * class CanActivateTeam implements CanActivate {
 *   constructor(private permissions: Permissions, private currentUser: UserToken) {}
 *
 *   canActivate(
 *     route: ActivatedRouteSnapshot,
 *     state: RouterStateSnapshot
 *   ): Observable<boolean|UrlTree>|Promise<boolean|UrlTree>|boolean|UrlTree {
 *     return this.permissions.canActivate(this.currentUser, route.params.id);
 *   }
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         canActivate: [CanActivateTeam]
 *       }
 *     ])
 *   ],
 *   providers: [CanActivateTeam, UserToken, Permissions]
 * })
 * class AppModule {}
 * ```
 *
 * You can alternatively provide a function with the `canActivate` signature:
 *
 * ```
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         canActivate: ['canActivateTeam']
 *       }
 *     ])
 *   ],
 *   providers: [
 *     {
 *       provide: 'canActivateTeam',
 *       useValue: (route: ActivatedRouteSnapshot, state: RouterStateSnapshot) => true
 *     }
 *   ]
 * })
 * class AppModule {}
 * ```
 *
 * \@publicApi
 * @record
 */
export function CanActivate() { }
if (false) {
    /**
     * @param {?} route
     * @param {?} state
     * @return {?}
     */
    CanActivate.prototype.canActivate = function (route, state) { };
}
/**
 * \@description
 *
 * Interface that a class can implement to be a guard deciding if a child route can be activated.
 * If all guards return `true`, navigation will continue. If any guard returns `false`,
 * navigation will be cancelled. If any guard returns a `UrlTree`, current navigation will
 * be cancelled and a new navigation will be kicked off to the `UrlTree` returned from the
 * guard.
 *
 * ```
 * class UserToken {}
 * class Permissions {
 *   canActivate(user: UserToken, id: string): boolean {
 *     return true;
 *   }
 * }
 *
 * \@Injectable()
 * class CanActivateTeam implements CanActivateChild {
 *   constructor(private permissions: Permissions, private currentUser: UserToken) {}
 *
 *   canActivateChild(
 *     route: ActivatedRouteSnapshot,
 *     state: RouterStateSnapshot
 *   ): Observable<boolean|UrlTree>|Promise<boolean|UrlTree>|boolean|UrlTree {
 *     return this.permissions.canActivate(this.currentUser, route.params.id);
 *   }
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'root',
 *         canActivateChild: [CanActivateTeam],
 *         children: [
 *           {
 *              path: 'team/:id',
 *              component: TeamComponent
 *           }
 *         ]
 *       }
 *     ])
 *   ],
 *   providers: [CanActivateTeam, UserToken, Permissions]
 * })
 * class AppModule {}
 * ```
 *
 * You can alternatively provide a function with the `canActivateChild` signature:
 *
 * ```
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'root',
 *         canActivateChild: ['canActivateTeam'],
 *         children: [
 *           {
 *             path: 'team/:id',
 *             component: TeamComponent
 *           }
 *         ]
 *       }
 *     ])
 *   ],
 *   providers: [
 *     {
 *       provide: 'canActivateTeam',
 *       useValue: (route: ActivatedRouteSnapshot, state: RouterStateSnapshot) => true
 *     }
 *   ]
 * })
 * class AppModule {}
 * ```
 *
 * \@publicApi
 * @record
 */
export function CanActivateChild() { }
if (false) {
    /**
     * @param {?} childRoute
     * @param {?} state
     * @return {?}
     */
    CanActivateChild.prototype.canActivateChild = function (childRoute, state) { };
}
/**
 * \@description
 *
 * Interface that a class can implement to be a guard deciding if a route can be deactivated.
 * If all guards return `true`, navigation will continue. If any guard returns `false`,
 * navigation will be cancelled. If any guard returns a `UrlTree`, current navigation will
 * be cancelled and a new navigation will be kicked off to the `UrlTree` returned from the
 * guard.
 *
 * ```
 * class UserToken {}
 * class Permissions {
 *   canDeactivate(user: UserToken, id: string): boolean {
 *     return true;
 *   }
 * }
 *
 * \@Injectable()
 * class CanDeactivateTeam implements CanDeactivate<TeamComponent> {
 *   constructor(private permissions: Permissions, private currentUser: UserToken) {}
 *
 *   canDeactivate(
 *     component: TeamComponent,
 *     currentRoute: ActivatedRouteSnapshot,
 *     currentState: RouterStateSnapshot,
 *     nextState: RouterStateSnapshot
 *   ): Observable<boolean|UrlTree>|Promise<boolean|UrlTree>|boolean|UrlTree {
 *     return this.permissions.canDeactivate(this.currentUser, route.params.id);
 *   }
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         canDeactivate: [CanDeactivateTeam]
 *       }
 *     ])
 *   ],
 *   providers: [CanDeactivateTeam, UserToken, Permissions]
 * })
 * class AppModule {}
 * ```
 *
 * You can alternatively provide a function with the `canDeactivate` signature:
 *
 * ```
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         canDeactivate: ['canDeactivateTeam']
 *       }
 *     ])
 *   ],
 *   providers: [
 *     {
 *       provide: 'canDeactivateTeam',
 *       useValue: (component: TeamComponent, currentRoute: ActivatedRouteSnapshot, currentState:
 * RouterStateSnapshot, nextState: RouterStateSnapshot) => true
 *     }
 *   ]
 * })
 * class AppModule {}
 * ```
 *
 * \@publicApi
 * @record
 * @template T
 */
export function CanDeactivate() { }
if (false) {
    /**
     * @param {?} component
     * @param {?} currentRoute
     * @param {?} currentState
     * @param {?=} nextState
     * @return {?}
     */
    CanDeactivate.prototype.canDeactivate = function (component, currentRoute, currentState, nextState) { };
}
/**
 * \@description
 *
 * Interface that classes can implement to be a data provider.
 * A data provider class can be used with the router to resolve data during navigation.
 * The interface defines a `resolve()` method that will be invoked when the navigation starts.
 * The router will then wait for the data to be resolved before the route is finally activated.
 *
 * ```
 * \@Injectable({ providedIn: 'root' })
 * export class HeroResolver implements Resolve<Hero> {
 *   constructor(private service: HeroService) {}
 *
 *   resolve(
 *     route: ActivatedRouteSnapshot,
 *     state: RouterStateSnapshot
 *   ): Observable<any>|Promise<any>|any {
 *     return this.service.getHero(route.paramMap.get('id'));
 *   }
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'detail/:id',
 *         component: HeroDetailComponent,
 *         resolve: {
 *           hero: HeroResolver
 *         }
 *       }
 *     ])
 *   ],
 *   exports: [RouterModule]
 * })
 * export class AppRoutingModule {}
 * ```
 *
 * You can alternatively provide a function with the `resolve` signature:
 *
 * ```
 * export const myHero: Hero = {
 *   // ...
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'detail/:id',
 *         component: HeroComponent,
 *         resolve: {
 *           hero: 'heroResolver'
 *         }
 *       }
 *     ])
 *   ],
 *   providers: [
 *     {
 *       provide: 'heroResolver',
 *       useValue: (route: ActivatedRouteSnapshot, state: RouterStateSnapshot) => myHero
 *     }
 *   ]
 * })
 * export class AppModule {}
 * ```
 *
 * \@publicApi
 * @record
 * @template T
 */
export function Resolve() { }
if (false) {
    /**
     * @param {?} route
     * @param {?} state
     * @return {?}
     */
    Resolve.prototype.resolve = function (route, state) { };
}
/**
 * \@description
 *
 * Interface that a class can implement to be a guard deciding if children can be loaded.
 *
 * ```
 * class UserToken {}
 * class Permissions {
 *   canLoadChildren(user: UserToken, id: string, segments: UrlSegment[]): boolean {
 *     return true;
 *   }
 * }
 *
 * \@Injectable()
 * class CanLoadTeamSection implements CanLoad {
 *   constructor(private permissions: Permissions, private currentUser: UserToken) {}
 *
 *   canLoad(route: Route, segments: UrlSegment[]): Observable<boolean>|Promise<boolean>|boolean {
 *     return this.permissions.canLoadChildren(this.currentUser, route, segments);
 *   }
 * }
 *
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         loadChildren: 'team.js',
 *         canLoad: [CanLoadTeamSection]
 *       }
 *     ])
 *   ],
 *   providers: [CanLoadTeamSection, UserToken, Permissions]
 * })
 * class AppModule {}
 * ```
 *
 * You can alternatively provide a function with the `canLoad` signature:
 *
 * ```
 * \@NgModule({
 *   imports: [
 *     RouterModule.forRoot([
 *       {
 *         path: 'team/:id',
 *         component: TeamComponent,
 *         loadChildren: 'team.js',
 *         canLoad: ['canLoadTeamSection']
 *       }
 *     ])
 *   ],
 *   providers: [
 *     {
 *       provide: 'canLoadTeamSection',
 *       useValue: (route: Route, segments: UrlSegment[]) => true
 *     }
 *   ]
 * })
 * class AppModule {}
 * ```
 *
 * \@publicApi
 * @record
 */
export function CanLoad() { }
if (false) {
    /**
     * @param {?} route
     * @param {?} segments
     * @return {?}
     */
    CanLoad.prototype.canLoad = function (route, segments) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW50ZXJmYWNlcy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvaW50ZXJmYWNlcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFvRkEsaUNBR0M7Ozs7Ozs7SUFGQyxnRUFDeUU7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFxRjNFLHNDQUdDOzs7Ozs7O0lBRkMsK0VBQ3lFOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBOEUzRSxtQ0FLQzs7Ozs7Ozs7O0lBSkMsd0dBR2E7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUE2RWYsNkJBRUM7Ozs7Ozs7SUFEQyx3REFBK0Y7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFvRWpHLDZCQUVDOzs7Ozs7O0lBREMsMkRBQTRGIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge09ic2VydmFibGV9IGZyb20gJ3J4anMnO1xuXG5pbXBvcnQge1JvdXRlfSBmcm9tICcuL2NvbmZpZyc7XG5pbXBvcnQge0FjdGl2YXRlZFJvdXRlU25hcHNob3QsIFJvdXRlclN0YXRlU25hcHNob3R9IGZyb20gJy4vcm91dGVyX3N0YXRlJztcbmltcG9ydCB7VXJsU2VnbWVudCwgVXJsVHJlZX0gZnJvbSAnLi91cmxfdHJlZSc7XG5cblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBJbnRlcmZhY2UgdGhhdCBhIGNsYXNzIGNhbiBpbXBsZW1lbnQgdG8gYmUgYSBndWFyZCBkZWNpZGluZyBpZiBhIHJvdXRlIGNhbiBiZSBhY3RpdmF0ZWQuXG4gKiBJZiBhbGwgZ3VhcmRzIHJldHVybiBgdHJ1ZWAsIG5hdmlnYXRpb24gd2lsbCBjb250aW51ZS4gSWYgYW55IGd1YXJkIHJldHVybnMgYGZhbHNlYCxcbiAqIG5hdmlnYXRpb24gd2lsbCBiZSBjYW5jZWxsZWQuIElmIGFueSBndWFyZCByZXR1cm5zIGEgYFVybFRyZWVgLCBjdXJyZW50IG5hdmlnYXRpb24gd2lsbFxuICogYmUgY2FuY2VsbGVkIGFuZCBhIG5ldyBuYXZpZ2F0aW9uIHdpbGwgYmUga2lja2VkIG9mZiB0byB0aGUgYFVybFRyZWVgIHJldHVybmVkIGZyb20gdGhlXG4gKiBndWFyZC5cbiAqXG4gKiBgYGBcbiAqIGNsYXNzIFVzZXJUb2tlbiB7fVxuICogY2xhc3MgUGVybWlzc2lvbnMge1xuICogICBjYW5BY3RpdmF0ZSh1c2VyOiBVc2VyVG9rZW4sIGlkOiBzdHJpbmcpOiBib29sZWFuIHtcbiAqICAgICByZXR1cm4gdHJ1ZTtcbiAqICAgfVxuICogfVxuICpcbiAqIEBJbmplY3RhYmxlKClcbiAqIGNsYXNzIENhbkFjdGl2YXRlVGVhbSBpbXBsZW1lbnRzIENhbkFjdGl2YXRlIHtcbiAqICAgY29uc3RydWN0b3IocHJpdmF0ZSBwZXJtaXNzaW9uczogUGVybWlzc2lvbnMsIHByaXZhdGUgY3VycmVudFVzZXI6IFVzZXJUb2tlbikge31cbiAqXG4gKiAgIGNhbkFjdGl2YXRlKFxuICogICAgIHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LFxuICogICAgIHN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90XG4gKiAgICk6IE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPnxQcm9taXNlPGJvb2xlYW58VXJsVHJlZT58Ym9vbGVhbnxVcmxUcmVlIHtcbiAqICAgICByZXR1cm4gdGhpcy5wZXJtaXNzaW9ucy5jYW5BY3RpdmF0ZSh0aGlzLmN1cnJlbnRVc2VyLCByb3V0ZS5wYXJhbXMuaWQpO1xuICogICB9XG4gKiB9XG4gKlxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgY29tcG9uZW50OiBUZWFtQ29tcG9uZW50LFxuICogICAgICAgICBjYW5BY3RpdmF0ZTogW0NhbkFjdGl2YXRlVGVhbV1cbiAqICAgICAgIH1cbiAqICAgICBdKVxuICogICBdLFxuICogICBwcm92aWRlcnM6IFtDYW5BY3RpdmF0ZVRlYW0sIFVzZXJUb2tlbiwgUGVybWlzc2lvbnNdXG4gKiB9KVxuICogY2xhc3MgQXBwTW9kdWxlIHt9XG4gKiBgYGBcbiAqXG4gKiBZb3UgY2FuIGFsdGVybmF0aXZlbHkgcHJvdmlkZSBhIGZ1bmN0aW9uIHdpdGggdGhlIGBjYW5BY3RpdmF0ZWAgc2lnbmF0dXJlOlxuICpcbiAqIGBgYFxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgY29tcG9uZW50OiBUZWFtQ29tcG9uZW50LFxuICogICAgICAgICBjYW5BY3RpdmF0ZTogWydjYW5BY3RpdmF0ZVRlYW0nXVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIHByb3ZpZGVyczogW1xuICogICAgIHtcbiAqICAgICAgIHByb3ZpZGU6ICdjYW5BY3RpdmF0ZVRlYW0nLFxuICogICAgICAgdXNlVmFsdWU6IChyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpID0+IHRydWVcbiAqICAgICB9XG4gKiAgIF1cbiAqIH0pXG4gKiBjbGFzcyBBcHBNb2R1bGUge31cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDYW5BY3RpdmF0ZSB7XG4gIGNhbkFjdGl2YXRlKHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBzdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCk6XG4gICAgICBPYnNlcnZhYmxlPGJvb2xlYW58VXJsVHJlZT58UHJvbWlzZTxib29sZWFufFVybFRyZWU+fGJvb2xlYW58VXJsVHJlZTtcbn1cblxuZXhwb3J0IHR5cGUgQ2FuQWN0aXZhdGVGbiA9IChyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpID0+XG4gICAgT2JzZXJ2YWJsZTxib29sZWFufFVybFRyZWU+fCBQcm9taXNlPGJvb2xlYW58VXJsVHJlZT58IGJvb2xlYW4gfCBVcmxUcmVlO1xuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIEludGVyZmFjZSB0aGF0IGEgY2xhc3MgY2FuIGltcGxlbWVudCB0byBiZSBhIGd1YXJkIGRlY2lkaW5nIGlmIGEgY2hpbGQgcm91dGUgY2FuIGJlIGFjdGl2YXRlZC5cbiAqIElmIGFsbCBndWFyZHMgcmV0dXJuIGB0cnVlYCwgbmF2aWdhdGlvbiB3aWxsIGNvbnRpbnVlLiBJZiBhbnkgZ3VhcmQgcmV0dXJucyBgZmFsc2VgLFxuICogbmF2aWdhdGlvbiB3aWxsIGJlIGNhbmNlbGxlZC4gSWYgYW55IGd1YXJkIHJldHVybnMgYSBgVXJsVHJlZWAsIGN1cnJlbnQgbmF2aWdhdGlvbiB3aWxsXG4gKiBiZSBjYW5jZWxsZWQgYW5kIGEgbmV3IG5hdmlnYXRpb24gd2lsbCBiZSBraWNrZWQgb2ZmIHRvIHRoZSBgVXJsVHJlZWAgcmV0dXJuZWQgZnJvbSB0aGVcbiAqIGd1YXJkLlxuICpcbiAqIGBgYFxuICogY2xhc3MgVXNlclRva2VuIHt9XG4gKiBjbGFzcyBQZXJtaXNzaW9ucyB7XG4gKiAgIGNhbkFjdGl2YXRlKHVzZXI6IFVzZXJUb2tlbiwgaWQ6IHN0cmluZyk6IGJvb2xlYW4ge1xuICogICAgIHJldHVybiB0cnVlO1xuICogICB9XG4gKiB9XG4gKlxuICogQEluamVjdGFibGUoKVxuICogY2xhc3MgQ2FuQWN0aXZhdGVUZWFtIGltcGxlbWVudHMgQ2FuQWN0aXZhdGVDaGlsZCB7XG4gKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgcGVybWlzc2lvbnM6IFBlcm1pc3Npb25zLCBwcml2YXRlIGN1cnJlbnRVc2VyOiBVc2VyVG9rZW4pIHt9XG4gKlxuICogICBjYW5BY3RpdmF0ZUNoaWxkKFxuICogICAgIHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LFxuICogICAgIHN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90XG4gKiAgICk6IE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPnxQcm9taXNlPGJvb2xlYW58VXJsVHJlZT58Ym9vbGVhbnxVcmxUcmVlIHtcbiAqICAgICByZXR1cm4gdGhpcy5wZXJtaXNzaW9ucy5jYW5BY3RpdmF0ZSh0aGlzLmN1cnJlbnRVc2VyLCByb3V0ZS5wYXJhbXMuaWQpO1xuICogICB9XG4gKiB9XG4gKlxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ3Jvb3QnLFxuICogICAgICAgICBjYW5BY3RpdmF0ZUNoaWxkOiBbQ2FuQWN0aXZhdGVUZWFtXSxcbiAqICAgICAgICAgY2hpbGRyZW46IFtcbiAqICAgICAgICAgICB7XG4gKiAgICAgICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgICAgICBjb21wb25lbnQ6IFRlYW1Db21wb25lbnRcbiAqICAgICAgICAgICB9XG4gKiAgICAgICAgIF1cbiAqICAgICAgIH1cbiAqICAgICBdKVxuICogICBdLFxuICogICBwcm92aWRlcnM6IFtDYW5BY3RpdmF0ZVRlYW0sIFVzZXJUb2tlbiwgUGVybWlzc2lvbnNdXG4gKiB9KVxuICogY2xhc3MgQXBwTW9kdWxlIHt9XG4gKiBgYGBcbiAqXG4gKiBZb3UgY2FuIGFsdGVybmF0aXZlbHkgcHJvdmlkZSBhIGZ1bmN0aW9uIHdpdGggdGhlIGBjYW5BY3RpdmF0ZUNoaWxkYCBzaWduYXR1cmU6XG4gKlxuICogYGBgXG4gKiBATmdNb2R1bGUoe1xuICogICBpbXBvcnRzOiBbXG4gKiAgICAgUm91dGVyTW9kdWxlLmZvclJvb3QoW1xuICogICAgICAge1xuICogICAgICAgICBwYXRoOiAncm9vdCcsXG4gKiAgICAgICAgIGNhbkFjdGl2YXRlQ2hpbGQ6IFsnY2FuQWN0aXZhdGVUZWFtJ10sXG4gKiAgICAgICAgIGNoaWxkcmVuOiBbXG4gKiAgICAgICAgICAge1xuICogICAgICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgICAgIGNvbXBvbmVudDogVGVhbUNvbXBvbmVudFxuICogICAgICAgICAgIH1cbiAqICAgICAgICAgXVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIHByb3ZpZGVyczogW1xuICogICAgIHtcbiAqICAgICAgIHByb3ZpZGU6ICdjYW5BY3RpdmF0ZVRlYW0nLFxuICogICAgICAgdXNlVmFsdWU6IChyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpID0+IHRydWVcbiAqICAgICB9XG4gKiAgIF1cbiAqIH0pXG4gKiBjbGFzcyBBcHBNb2R1bGUge31cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDYW5BY3RpdmF0ZUNoaWxkIHtcbiAgY2FuQWN0aXZhdGVDaGlsZChjaGlsZFJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBzdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCk6XG4gICAgICBPYnNlcnZhYmxlPGJvb2xlYW58VXJsVHJlZT58UHJvbWlzZTxib29sZWFufFVybFRyZWU+fGJvb2xlYW58VXJsVHJlZTtcbn1cblxuZXhwb3J0IHR5cGUgQ2FuQWN0aXZhdGVDaGlsZEZuID0gKGNoaWxkUm91dGU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIHN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90KSA9PlxuICAgIE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPnwgUHJvbWlzZTxib29sZWFufFVybFRyZWU+fCBib29sZWFuIHwgVXJsVHJlZTtcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBJbnRlcmZhY2UgdGhhdCBhIGNsYXNzIGNhbiBpbXBsZW1lbnQgdG8gYmUgYSBndWFyZCBkZWNpZGluZyBpZiBhIHJvdXRlIGNhbiBiZSBkZWFjdGl2YXRlZC5cbiAqIElmIGFsbCBndWFyZHMgcmV0dXJuIGB0cnVlYCwgbmF2aWdhdGlvbiB3aWxsIGNvbnRpbnVlLiBJZiBhbnkgZ3VhcmQgcmV0dXJucyBgZmFsc2VgLFxuICogbmF2aWdhdGlvbiB3aWxsIGJlIGNhbmNlbGxlZC4gSWYgYW55IGd1YXJkIHJldHVybnMgYSBgVXJsVHJlZWAsIGN1cnJlbnQgbmF2aWdhdGlvbiB3aWxsXG4gKiBiZSBjYW5jZWxsZWQgYW5kIGEgbmV3IG5hdmlnYXRpb24gd2lsbCBiZSBraWNrZWQgb2ZmIHRvIHRoZSBgVXJsVHJlZWAgcmV0dXJuZWQgZnJvbSB0aGVcbiAqIGd1YXJkLlxuICpcbiAqIGBgYFxuICogY2xhc3MgVXNlclRva2VuIHt9XG4gKiBjbGFzcyBQZXJtaXNzaW9ucyB7XG4gKiAgIGNhbkRlYWN0aXZhdGUodXNlcjogVXNlclRva2VuLCBpZDogc3RyaW5nKTogYm9vbGVhbiB7XG4gKiAgICAgcmV0dXJuIHRydWU7XG4gKiAgIH1cbiAqIH1cbiAqXG4gKiBASW5qZWN0YWJsZSgpXG4gKiBjbGFzcyBDYW5EZWFjdGl2YXRlVGVhbSBpbXBsZW1lbnRzIENhbkRlYWN0aXZhdGU8VGVhbUNvbXBvbmVudD4ge1xuICogICBjb25zdHJ1Y3Rvcihwcml2YXRlIHBlcm1pc3Npb25zOiBQZXJtaXNzaW9ucywgcHJpdmF0ZSBjdXJyZW50VXNlcjogVXNlclRva2VuKSB7fVxuICpcbiAqICAgY2FuRGVhY3RpdmF0ZShcbiAqICAgICBjb21wb25lbnQ6IFRlYW1Db21wb25lbnQsXG4gKiAgICAgY3VycmVudFJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LFxuICogICAgIGN1cnJlbnRTdGF0ZTogUm91dGVyU3RhdGVTbmFwc2hvdCxcbiAqICAgICBuZXh0U3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3RcbiAqICAgKTogT2JzZXJ2YWJsZTxib29sZWFufFVybFRyZWU+fFByb21pc2U8Ym9vbGVhbnxVcmxUcmVlPnxib29sZWFufFVybFRyZWUge1xuICogICAgIHJldHVybiB0aGlzLnBlcm1pc3Npb25zLmNhbkRlYWN0aXZhdGUodGhpcy5jdXJyZW50VXNlciwgcm91dGUucGFyYW1zLmlkKTtcbiAqICAgfVxuICogfVxuICpcbiAqIEBOZ01vZHVsZSh7XG4gKiAgIGltcG9ydHM6IFtcbiAqICAgICBSb3V0ZXJNb2R1bGUuZm9yUm9vdChbXG4gKiAgICAgICB7XG4gKiAgICAgICAgIHBhdGg6ICd0ZWFtLzppZCcsXG4gKiAgICAgICAgIGNvbXBvbmVudDogVGVhbUNvbXBvbmVudCxcbiAqICAgICAgICAgY2FuRGVhY3RpdmF0ZTogW0NhbkRlYWN0aXZhdGVUZWFtXVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIHByb3ZpZGVyczogW0NhbkRlYWN0aXZhdGVUZWFtLCBVc2VyVG9rZW4sIFBlcm1pc3Npb25zXVxuICogfSlcbiAqIGNsYXNzIEFwcE1vZHVsZSB7fVxuICogYGBgXG4gKlxuICogWW91IGNhbiBhbHRlcm5hdGl2ZWx5IHByb3ZpZGUgYSBmdW5jdGlvbiB3aXRoIHRoZSBgY2FuRGVhY3RpdmF0ZWAgc2lnbmF0dXJlOlxuICpcbiAqIGBgYFxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgY29tcG9uZW50OiBUZWFtQ29tcG9uZW50LFxuICogICAgICAgICBjYW5EZWFjdGl2YXRlOiBbJ2NhbkRlYWN0aXZhdGVUZWFtJ11cbiAqICAgICAgIH1cbiAqICAgICBdKVxuICogICBdLFxuICogICBwcm92aWRlcnM6IFtcbiAqICAgICB7XG4gKiAgICAgICBwcm92aWRlOiAnY2FuRGVhY3RpdmF0ZVRlYW0nLFxuICogICAgICAgdXNlVmFsdWU6IChjb21wb25lbnQ6IFRlYW1Db21wb25lbnQsIGN1cnJlbnRSb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgY3VycmVudFN0YXRlOlxuICogUm91dGVyU3RhdGVTbmFwc2hvdCwgbmV4dFN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90KSA9PiB0cnVlXG4gKiAgICAgfVxuICogICBdXG4gKiB9KVxuICogY2xhc3MgQXBwTW9kdWxlIHt9XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgQ2FuRGVhY3RpdmF0ZTxUPiB7XG4gIGNhbkRlYWN0aXZhdGUoXG4gICAgICBjb21wb25lbnQ6IFQsIGN1cnJlbnRSb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgY3VycmVudFN0YXRlOiBSb3V0ZXJTdGF0ZVNuYXBzaG90LFxuICAgICAgbmV4dFN0YXRlPzogUm91dGVyU3RhdGVTbmFwc2hvdCk6IE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPnxQcm9taXNlPGJvb2xlYW58VXJsVHJlZT58Ym9vbGVhblxuICAgICAgfFVybFRyZWU7XG59XG5cbmV4cG9ydCB0eXBlIENhbkRlYWN0aXZhdGVGbjxUPiA9XG4gICAgKGNvbXBvbmVudDogVCwgY3VycmVudFJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBjdXJyZW50U3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QsXG4gICAgIG5leHRTdGF0ZT86IFJvdXRlclN0YXRlU25hcHNob3QpID0+XG4gICAgICAgIE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPnwgUHJvbWlzZTxib29sZWFufFVybFRyZWU+fCBib29sZWFuIHwgVXJsVHJlZTtcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBJbnRlcmZhY2UgdGhhdCBjbGFzc2VzIGNhbiBpbXBsZW1lbnQgdG8gYmUgYSBkYXRhIHByb3ZpZGVyLlxuICogQSBkYXRhIHByb3ZpZGVyIGNsYXNzIGNhbiBiZSB1c2VkIHdpdGggdGhlIHJvdXRlciB0byByZXNvbHZlIGRhdGEgZHVyaW5nIG5hdmlnYXRpb24uXG4gKiBUaGUgaW50ZXJmYWNlIGRlZmluZXMgYSBgcmVzb2x2ZSgpYCBtZXRob2QgdGhhdCB3aWxsIGJlIGludm9rZWQgd2hlbiB0aGUgbmF2aWdhdGlvbiBzdGFydHMuXG4gKiBUaGUgcm91dGVyIHdpbGwgdGhlbiB3YWl0IGZvciB0aGUgZGF0YSB0byBiZSByZXNvbHZlZCBiZWZvcmUgdGhlIHJvdXRlIGlzIGZpbmFsbHkgYWN0aXZhdGVkLlxuICpcbiAqIGBgYFxuICogQEluamVjdGFibGUoeyBwcm92aWRlZEluOiAncm9vdCcgfSlcbiAqIGV4cG9ydCBjbGFzcyBIZXJvUmVzb2x2ZXIgaW1wbGVtZW50cyBSZXNvbHZlPEhlcm8+IHtcbiAqICAgY29uc3RydWN0b3IocHJpdmF0ZSBzZXJ2aWNlOiBIZXJvU2VydmljZSkge31cbiAqXG4gKiAgIHJlc29sdmUoXG4gKiAgICAgcm91dGU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsXG4gKiAgICAgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3RcbiAqICAgKTogT2JzZXJ2YWJsZTxhbnk+fFByb21pc2U8YW55Pnxhbnkge1xuICogICAgIHJldHVybiB0aGlzLnNlcnZpY2UuZ2V0SGVybyhyb3V0ZS5wYXJhbU1hcC5nZXQoJ2lkJykpO1xuICogICB9XG4gKiB9XG4gKlxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ2RldGFpbC86aWQnLFxuICogICAgICAgICBjb21wb25lbnQ6IEhlcm9EZXRhaWxDb21wb25lbnQsXG4gKiAgICAgICAgIHJlc29sdmU6IHtcbiAqICAgICAgICAgICBoZXJvOiBIZXJvUmVzb2x2ZXJcbiAqICAgICAgICAgfVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIGV4cG9ydHM6IFtSb3V0ZXJNb2R1bGVdXG4gKiB9KVxuICogZXhwb3J0IGNsYXNzIEFwcFJvdXRpbmdNb2R1bGUge31cbiAqIGBgYFxuICpcbiAqIFlvdSBjYW4gYWx0ZXJuYXRpdmVseSBwcm92aWRlIGEgZnVuY3Rpb24gd2l0aCB0aGUgYHJlc29sdmVgIHNpZ25hdHVyZTpcbiAqXG4gKiBgYGBcbiAqIGV4cG9ydCBjb25zdCBteUhlcm86IEhlcm8gPSB7XG4gKiAgIC8vIC4uLlxuICogfVxuICpcbiAqIEBOZ01vZHVsZSh7XG4gKiAgIGltcG9ydHM6IFtcbiAqICAgICBSb3V0ZXJNb2R1bGUuZm9yUm9vdChbXG4gKiAgICAgICB7XG4gKiAgICAgICAgIHBhdGg6ICdkZXRhaWwvOmlkJyxcbiAqICAgICAgICAgY29tcG9uZW50OiBIZXJvQ29tcG9uZW50LFxuICogICAgICAgICByZXNvbHZlOiB7XG4gKiAgICAgICAgICAgaGVybzogJ2hlcm9SZXNvbHZlcidcbiAqICAgICAgICAgfVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIHByb3ZpZGVyczogW1xuICogICAgIHtcbiAqICAgICAgIHByb3ZpZGU6ICdoZXJvUmVzb2x2ZXInLFxuICogICAgICAgdXNlVmFsdWU6IChyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpID0+IG15SGVyb1xuICogICAgIH1cbiAqICAgXVxuICogfSlcbiAqIGV4cG9ydCBjbGFzcyBBcHBNb2R1bGUge31cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSZXNvbHZlPFQ+IHtcbiAgcmVzb2x2ZShyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgc3RhdGU6IFJvdXRlclN0YXRlU25hcHNob3QpOiBPYnNlcnZhYmxlPFQ+fFByb21pc2U8VD58VDtcbn1cblxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIEludGVyZmFjZSB0aGF0IGEgY2xhc3MgY2FuIGltcGxlbWVudCB0byBiZSBhIGd1YXJkIGRlY2lkaW5nIGlmIGNoaWxkcmVuIGNhbiBiZSBsb2FkZWQuXG4gKlxuICogYGBgXG4gKiBjbGFzcyBVc2VyVG9rZW4ge31cbiAqIGNsYXNzIFBlcm1pc3Npb25zIHtcbiAqICAgY2FuTG9hZENoaWxkcmVuKHVzZXI6IFVzZXJUb2tlbiwgaWQ6IHN0cmluZywgc2VnbWVudHM6IFVybFNlZ21lbnRbXSk6IGJvb2xlYW4ge1xuICogICAgIHJldHVybiB0cnVlO1xuICogICB9XG4gKiB9XG4gKlxuICogQEluamVjdGFibGUoKVxuICogY2xhc3MgQ2FuTG9hZFRlYW1TZWN0aW9uIGltcGxlbWVudHMgQ2FuTG9hZCB7XG4gKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgcGVybWlzc2lvbnM6IFBlcm1pc3Npb25zLCBwcml2YXRlIGN1cnJlbnRVc2VyOiBVc2VyVG9rZW4pIHt9XG4gKlxuICogICBjYW5Mb2FkKHJvdXRlOiBSb3V0ZSwgc2VnbWVudHM6IFVybFNlZ21lbnRbXSk6IE9ic2VydmFibGU8Ym9vbGVhbj58UHJvbWlzZTxib29sZWFuPnxib29sZWFuIHtcbiAqICAgICByZXR1cm4gdGhpcy5wZXJtaXNzaW9ucy5jYW5Mb2FkQ2hpbGRyZW4odGhpcy5jdXJyZW50VXNlciwgcm91dGUsIHNlZ21lbnRzKTtcbiAqICAgfVxuICogfVxuICpcbiAqIEBOZ01vZHVsZSh7XG4gKiAgIGltcG9ydHM6IFtcbiAqICAgICBSb3V0ZXJNb2R1bGUuZm9yUm9vdChbXG4gKiAgICAgICB7XG4gKiAgICAgICAgIHBhdGg6ICd0ZWFtLzppZCcsXG4gKiAgICAgICAgIGNvbXBvbmVudDogVGVhbUNvbXBvbmVudCxcbiAqICAgICAgICAgbG9hZENoaWxkcmVuOiAndGVhbS5qcycsXG4gKiAgICAgICAgIGNhbkxvYWQ6IFtDYW5Mb2FkVGVhbVNlY3Rpb25dXG4gKiAgICAgICB9XG4gKiAgICAgXSlcbiAqICAgXSxcbiAqICAgcHJvdmlkZXJzOiBbQ2FuTG9hZFRlYW1TZWN0aW9uLCBVc2VyVG9rZW4sIFBlcm1pc3Npb25zXVxuICogfSlcbiAqIGNsYXNzIEFwcE1vZHVsZSB7fVxuICogYGBgXG4gKlxuICogWW91IGNhbiBhbHRlcm5hdGl2ZWx5IHByb3ZpZGUgYSBmdW5jdGlvbiB3aXRoIHRoZSBgY2FuTG9hZGAgc2lnbmF0dXJlOlxuICpcbiAqIGBgYFxuICogQE5nTW9kdWxlKHtcbiAqICAgaW1wb3J0czogW1xuICogICAgIFJvdXRlck1vZHVsZS5mb3JSb290KFtcbiAqICAgICAgIHtcbiAqICAgICAgICAgcGF0aDogJ3RlYW0vOmlkJyxcbiAqICAgICAgICAgY29tcG9uZW50OiBUZWFtQ29tcG9uZW50LFxuICogICAgICAgICBsb2FkQ2hpbGRyZW46ICd0ZWFtLmpzJyxcbiAqICAgICAgICAgY2FuTG9hZDogWydjYW5Mb2FkVGVhbVNlY3Rpb24nXVxuICogICAgICAgfVxuICogICAgIF0pXG4gKiAgIF0sXG4gKiAgIHByb3ZpZGVyczogW1xuICogICAgIHtcbiAqICAgICAgIHByb3ZpZGU6ICdjYW5Mb2FkVGVhbVNlY3Rpb24nLFxuICogICAgICAgdXNlVmFsdWU6IChyb3V0ZTogUm91dGUsIHNlZ21lbnRzOiBVcmxTZWdtZW50W10pID0+IHRydWVcbiAqICAgICB9XG4gKiAgIF1cbiAqIH0pXG4gKiBjbGFzcyBBcHBNb2R1bGUge31cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDYW5Mb2FkIHtcbiAgY2FuTG9hZChyb3V0ZTogUm91dGUsIHNlZ21lbnRzOiBVcmxTZWdtZW50W10pOiBPYnNlcnZhYmxlPGJvb2xlYW4+fFByb21pc2U8Ym9vbGVhbj58Ym9vbGVhbjtcbn1cblxuZXhwb3J0IHR5cGUgQ2FuTG9hZEZuID0gKHJvdXRlOiBSb3V0ZSwgc2VnbWVudHM6IFVybFNlZ21lbnRbXSkgPT5cbiAgICBPYnNlcnZhYmxlPGJvb2xlYW4+fCBQcm9taXNlPGJvb2xlYW4+fCBib29sZWFuO1xuIl19