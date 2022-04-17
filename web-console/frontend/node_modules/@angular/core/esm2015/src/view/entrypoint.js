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
import { Injector } from '../di/injector';
import { NgModuleFactory } from '../linker/ng_module_factory';
import { initServicesIfNeeded } from './services';
import { Services } from './types';
import { resolveDefinition } from './util';
/**
 * @param {?} override
 * @return {?}
 */
export function overrideProvider(override) {
    initServicesIfNeeded();
    return Services.overrideProvider(override);
}
/**
 * @param {?} comp
 * @param {?} componentFactory
 * @return {?}
 */
export function overrideComponentView(comp, componentFactory) {
    initServicesIfNeeded();
    return Services.overrideComponentView(comp, componentFactory);
}
/**
 * @return {?}
 */
export function clearOverrides() {
    initServicesIfNeeded();
    return Services.clearOverrides();
}
// Attention: this function is called as top level function.
// Putting any logic in here will destroy closure tree shaking!
/**
 * @param {?} ngModuleType
 * @param {?} bootstrapComponents
 * @param {?} defFactory
 * @return {?}
 */
export function createNgModuleFactory(ngModuleType, bootstrapComponents, defFactory) {
    return new NgModuleFactory_(ngModuleType, bootstrapComponents, defFactory);
}
/**
 * @param {?} def
 * @return {?}
 */
function cloneNgModuleDefinition(def) {
    /** @type {?} */
    const providers = Array.from(def.providers);
    /** @type {?} */
    const modules = Array.from(def.modules);
    /** @type {?} */
    const providersByKey = {};
    for (const key in def.providersByKey) {
        providersByKey[key] = def.providersByKey[key];
    }
    return {
        factory: def.factory,
        isRoot: def.isRoot, providers, modules, providersByKey,
    };
}
class NgModuleFactory_ extends NgModuleFactory {
    /**
     * @param {?} moduleType
     * @param {?} _bootstrapComponents
     * @param {?} _ngModuleDefFactory
     */
    constructor(moduleType, _bootstrapComponents, _ngModuleDefFactory) {
        // Attention: this ctor is called as top level function.
        // Putting any logic in here will destroy closure tree shaking!
        super();
        this.moduleType = moduleType;
        this._bootstrapComponents = _bootstrapComponents;
        this._ngModuleDefFactory = _ngModuleDefFactory;
    }
    /**
     * @param {?} parentInjector
     * @return {?}
     */
    create(parentInjector) {
        initServicesIfNeeded();
        // Clone the NgModuleDefinition so that any tree shakeable provider definition
        // added to this instance of the NgModuleRef doesn't affect the cached copy.
        // See https://github.com/angular/angular/issues/25018.
        /** @type {?} */
        const def = cloneNgModuleDefinition(resolveDefinition(this._ngModuleDefFactory));
        return Services.createNgModuleRef(this.moduleType, parentInjector || Injector.NULL, this._bootstrapComponents, def);
    }
}
if (false) {
    /** @type {?} */
    NgModuleFactory_.prototype.moduleType;
    /**
     * @type {?}
     * @private
     */
    NgModuleFactory_.prototype._bootstrapComponents;
    /**
     * @type {?}
     * @private
     */
    NgModuleFactory_.prototype._ngModuleDefFactory;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZW50cnlwb2ludC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvZW50cnlwb2ludC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUd4QyxPQUFPLEVBQUMsZUFBZSxFQUFjLE1BQU0sNkJBQTZCLENBQUM7QUFFekUsT0FBTyxFQUFDLG9CQUFvQixFQUFDLE1BQU0sWUFBWSxDQUFDO0FBQ2hELE9BQU8sRUFBdUYsUUFBUSxFQUFpQixNQUFNLFNBQVMsQ0FBQztBQUN2SSxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxRQUFRLENBQUM7Ozs7O0FBRXpDLE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxRQUEwQjtJQUN6RCxvQkFBb0IsRUFBRSxDQUFDO0lBQ3ZCLE9BQU8sUUFBUSxDQUFDLGdCQUFnQixDQUFDLFFBQVEsQ0FBQyxDQUFDO0FBQzdDLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxxQkFBcUIsQ0FBQyxJQUFlLEVBQUUsZ0JBQXVDO0lBQzVGLG9CQUFvQixFQUFFLENBQUM7SUFDdkIsT0FBTyxRQUFRLENBQUMscUJBQXFCLENBQUMsSUFBSSxFQUFFLGdCQUFnQixDQUFDLENBQUM7QUFDaEUsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSxjQUFjO0lBQzVCLG9CQUFvQixFQUFFLENBQUM7SUFDdkIsT0FBTyxRQUFRLENBQUMsY0FBYyxFQUFFLENBQUM7QUFDbkMsQ0FBQzs7Ozs7Ozs7O0FBSUQsTUFBTSxVQUFVLHFCQUFxQixDQUNqQyxZQUF1QixFQUFFLG1CQUFnQyxFQUN6RCxVQUFxQztJQUN2QyxPQUFPLElBQUksZ0JBQWdCLENBQUMsWUFBWSxFQUFFLG1CQUFtQixFQUFFLFVBQVUsQ0FBQyxDQUFDO0FBQzdFLENBQUM7Ozs7O0FBRUQsU0FBUyx1QkFBdUIsQ0FBQyxHQUF1Qjs7VUFDaEQsU0FBUyxHQUFHLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQzs7VUFDckMsT0FBTyxHQUFHLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQzs7VUFDakMsY0FBYyxHQUE4QyxFQUFFO0lBQ3BFLEtBQUssTUFBTSxHQUFHLElBQUksR0FBRyxDQUFDLGNBQWMsRUFBRTtRQUNwQyxjQUFjLENBQUMsR0FBRyxDQUFDLEdBQUcsR0FBRyxDQUFDLGNBQWMsQ0FBQyxHQUFHLENBQUMsQ0FBQztLQUMvQztJQUVELE9BQU87UUFDTCxPQUFPLEVBQUUsR0FBRyxDQUFDLE9BQU87UUFDcEIsTUFBTSxFQUFFLEdBQUcsQ0FBQyxNQUFNLEVBQUUsU0FBUyxFQUFFLE9BQU8sRUFBRSxjQUFjO0tBQ3ZELENBQUM7QUFDSixDQUFDO0FBRUQsTUFBTSxnQkFBaUIsU0FBUSxlQUFvQjs7Ozs7O0lBQ2pELFlBQ29CLFVBQXFCLEVBQVUsb0JBQWlDLEVBQ3hFLG1CQUE4QztRQUN4RCx3REFBd0Q7UUFDeEQsK0RBQStEO1FBQy9ELEtBQUssRUFBRSxDQUFDO1FBSlUsZUFBVSxHQUFWLFVBQVUsQ0FBVztRQUFVLHlCQUFvQixHQUFwQixvQkFBb0IsQ0FBYTtRQUN4RSx3QkFBbUIsR0FBbkIsbUJBQW1CLENBQTJCO0lBSTFELENBQUM7Ozs7O0lBRUQsTUFBTSxDQUFDLGNBQTZCO1FBQ2xDLG9CQUFvQixFQUFFLENBQUM7Ozs7O2NBSWpCLEdBQUcsR0FBRyx1QkFBdUIsQ0FBQyxpQkFBaUIsQ0FBQyxJQUFJLENBQUMsbUJBQW1CLENBQUMsQ0FBQztRQUNoRixPQUFPLFFBQVEsQ0FBQyxpQkFBaUIsQ0FDN0IsSUFBSSxDQUFDLFVBQVUsRUFBRSxjQUFjLElBQUksUUFBUSxDQUFDLElBQUksRUFBRSxJQUFJLENBQUMsb0JBQW9CLEVBQUUsR0FBRyxDQUFDLENBQUM7SUFDeEYsQ0FBQztDQUNGOzs7SUFoQkssc0NBQXFDOzs7OztJQUFFLGdEQUF5Qzs7Ozs7SUFDaEYsK0NBQXNEIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICcuLi9kaS9pbmplY3Rvcic7XG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7Q29tcG9uZW50RmFjdG9yeX0gZnJvbSAnLi4vbGlua2VyL2NvbXBvbmVudF9mYWN0b3J5JztcbmltcG9ydCB7TmdNb2R1bGVGYWN0b3J5LCBOZ01vZHVsZVJlZn0gZnJvbSAnLi4vbGlua2VyL25nX21vZHVsZV9mYWN0b3J5JztcblxuaW1wb3J0IHtpbml0U2VydmljZXNJZk5lZWRlZH0gZnJvbSAnLi9zZXJ2aWNlcyc7XG5pbXBvcnQge05nTW9kdWxlRGVmaW5pdGlvbiwgTmdNb2R1bGVEZWZpbml0aW9uRmFjdG9yeSwgTmdNb2R1bGVQcm92aWRlckRlZiwgUHJvdmlkZXJPdmVycmlkZSwgU2VydmljZXMsIFZpZXdEZWZpbml0aW9ufSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7cmVzb2x2ZURlZmluaXRpb259IGZyb20gJy4vdXRpbCc7XG5cbmV4cG9ydCBmdW5jdGlvbiBvdmVycmlkZVByb3ZpZGVyKG92ZXJyaWRlOiBQcm92aWRlck92ZXJyaWRlKSB7XG4gIGluaXRTZXJ2aWNlc0lmTmVlZGVkKCk7XG4gIHJldHVybiBTZXJ2aWNlcy5vdmVycmlkZVByb3ZpZGVyKG92ZXJyaWRlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG92ZXJyaWRlQ29tcG9uZW50Vmlldyhjb21wOiBUeXBlPGFueT4sIGNvbXBvbmVudEZhY3Rvcnk6IENvbXBvbmVudEZhY3Rvcnk8YW55Pikge1xuICBpbml0U2VydmljZXNJZk5lZWRlZCgpO1xuICByZXR1cm4gU2VydmljZXMub3ZlcnJpZGVDb21wb25lbnRWaWV3KGNvbXAsIGNvbXBvbmVudEZhY3RvcnkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJPdmVycmlkZXMoKSB7XG4gIGluaXRTZXJ2aWNlc0lmTmVlZGVkKCk7XG4gIHJldHVybiBTZXJ2aWNlcy5jbGVhck92ZXJyaWRlcygpO1xufVxuXG4vLyBBdHRlbnRpb246IHRoaXMgZnVuY3Rpb24gaXMgY2FsbGVkIGFzIHRvcCBsZXZlbCBmdW5jdGlvbi5cbi8vIFB1dHRpbmcgYW55IGxvZ2ljIGluIGhlcmUgd2lsbCBkZXN0cm95IGNsb3N1cmUgdHJlZSBzaGFraW5nIVxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZU5nTW9kdWxlRmFjdG9yeShcbiAgICBuZ01vZHVsZVR5cGU6IFR5cGU8YW55PiwgYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10sXG4gICAgZGVmRmFjdG9yeTogTmdNb2R1bGVEZWZpbml0aW9uRmFjdG9yeSk6IE5nTW9kdWxlRmFjdG9yeTxhbnk+IHtcbiAgcmV0dXJuIG5ldyBOZ01vZHVsZUZhY3RvcnlfKG5nTW9kdWxlVHlwZSwgYm9vdHN0cmFwQ29tcG9uZW50cywgZGVmRmFjdG9yeSk7XG59XG5cbmZ1bmN0aW9uIGNsb25lTmdNb2R1bGVEZWZpbml0aW9uKGRlZjogTmdNb2R1bGVEZWZpbml0aW9uKTogTmdNb2R1bGVEZWZpbml0aW9uIHtcbiAgY29uc3QgcHJvdmlkZXJzID0gQXJyYXkuZnJvbShkZWYucHJvdmlkZXJzKTtcbiAgY29uc3QgbW9kdWxlcyA9IEFycmF5LmZyb20oZGVmLm1vZHVsZXMpO1xuICBjb25zdCBwcm92aWRlcnNCeUtleToge1t0b2tlbktleTogc3RyaW5nXTogTmdNb2R1bGVQcm92aWRlckRlZn0gPSB7fTtcbiAgZm9yIChjb25zdCBrZXkgaW4gZGVmLnByb3ZpZGVyc0J5S2V5KSB7XG4gICAgcHJvdmlkZXJzQnlLZXlba2V5XSA9IGRlZi5wcm92aWRlcnNCeUtleVtrZXldO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBmYWN0b3J5OiBkZWYuZmFjdG9yeSxcbiAgICBpc1Jvb3Q6IGRlZi5pc1Jvb3QsIHByb3ZpZGVycywgbW9kdWxlcywgcHJvdmlkZXJzQnlLZXksXG4gIH07XG59XG5cbmNsYXNzIE5nTW9kdWxlRmFjdG9yeV8gZXh0ZW5kcyBOZ01vZHVsZUZhY3Rvcnk8YW55PiB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHJlYWRvbmx5IG1vZHVsZVR5cGU6IFR5cGU8YW55PiwgcHJpdmF0ZSBfYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10sXG4gICAgICBwcml2YXRlIF9uZ01vZHVsZURlZkZhY3Rvcnk6IE5nTW9kdWxlRGVmaW5pdGlvbkZhY3RvcnkpIHtcbiAgICAvLyBBdHRlbnRpb246IHRoaXMgY3RvciBpcyBjYWxsZWQgYXMgdG9wIGxldmVsIGZ1bmN0aW9uLlxuICAgIC8vIFB1dHRpbmcgYW55IGxvZ2ljIGluIGhlcmUgd2lsbCBkZXN0cm95IGNsb3N1cmUgdHJlZSBzaGFraW5nIVxuICAgIHN1cGVyKCk7XG4gIH1cblxuICBjcmVhdGUocGFyZW50SW5qZWN0b3I6IEluamVjdG9yfG51bGwpOiBOZ01vZHVsZVJlZjxhbnk+IHtcbiAgICBpbml0U2VydmljZXNJZk5lZWRlZCgpO1xuICAgIC8vIENsb25lIHRoZSBOZ01vZHVsZURlZmluaXRpb24gc28gdGhhdCBhbnkgdHJlZSBzaGFrZWFibGUgcHJvdmlkZXIgZGVmaW5pdGlvblxuICAgIC8vIGFkZGVkIHRvIHRoaXMgaW5zdGFuY2Ugb2YgdGhlIE5nTW9kdWxlUmVmIGRvZXNuJ3QgYWZmZWN0IHRoZSBjYWNoZWQgY29weS5cbiAgICAvLyBTZWUgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9pc3N1ZXMvMjUwMTguXG4gICAgY29uc3QgZGVmID0gY2xvbmVOZ01vZHVsZURlZmluaXRpb24ocmVzb2x2ZURlZmluaXRpb24odGhpcy5fbmdNb2R1bGVEZWZGYWN0b3J5KSk7XG4gICAgcmV0dXJuIFNlcnZpY2VzLmNyZWF0ZU5nTW9kdWxlUmVmKFxuICAgICAgICB0aGlzLm1vZHVsZVR5cGUsIHBhcmVudEluamVjdG9yIHx8IEluamVjdG9yLk5VTEwsIHRoaXMuX2Jvb3RzdHJhcENvbXBvbmVudHMsIGRlZik7XG4gIH1cbn1cbiJdfQ==