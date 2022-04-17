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
import { ReflectionCapabilities } from '../reflection/reflection_capabilities';
import { getClosureSafeProperty } from '../util/property';
import { injectArgs, ɵɵinject } from './injector_compatibility';
const ɵ0 = getClosureSafeProperty;
/** @type {?} */
const USE_VALUE = getClosureSafeProperty({ provide: String, useValue: ɵ0 });
/** @type {?} */
const EMPTY_ARRAY = [];
/**
 * @param {?} type
 * @param {?=} provider
 * @return {?}
 */
export function convertInjectableProviderToFactory(type, provider) {
    if (!provider) {
        /** @type {?} */
        const reflectionCapabilities = new ReflectionCapabilities();
        /** @type {?} */
        const deps = reflectionCapabilities.parameters(type);
        // TODO - convert to flags.
        return (/**
         * @return {?}
         */
        () => new type(...injectArgs((/** @type {?} */ (deps)))));
    }
    if (USE_VALUE in provider) {
        /** @type {?} */
        const valueProvider = ((/** @type {?} */ (provider)));
        return (/**
         * @return {?}
         */
        () => valueProvider.useValue);
    }
    else if (((/** @type {?} */ (provider))).useExisting) {
        /** @type {?} */
        const existingProvider = ((/** @type {?} */ (provider)));
        return (/**
         * @return {?}
         */
        () => ɵɵinject(existingProvider.useExisting));
    }
    else if (((/** @type {?} */ (provider))).useFactory) {
        /** @type {?} */
        const factoryProvider = ((/** @type {?} */ (provider)));
        return (/**
         * @return {?}
         */
        () => factoryProvider.useFactory(...injectArgs(factoryProvider.deps || EMPTY_ARRAY)));
    }
    else if (((/** @type {?} */ (provider))).useClass) {
        /** @type {?} */
        const classProvider = ((/** @type {?} */ (provider)));
        /** @type {?} */
        let deps = ((/** @type {?} */ (provider))).deps;
        if (!deps) {
            /** @type {?} */
            const reflectionCapabilities = new ReflectionCapabilities();
            deps = reflectionCapabilities.parameters(type);
        }
        return (/**
         * @return {?}
         */
        () => new classProvider.useClass(...injectArgs(deps)));
    }
    else {
        /** @type {?} */
        let deps = ((/** @type {?} */ (provider))).deps;
        if (!deps) {
            /** @type {?} */
            const reflectionCapabilities = new ReflectionCapabilities();
            deps = reflectionCapabilities.parameters(type);
        }
        return (/**
         * @return {?}
         */
        () => new type(...injectArgs((/** @type {?} */ (deps)))));
    }
}
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2RpL3V0aWwudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFTQSxPQUFPLEVBQUMsc0JBQXNCLEVBQUMsTUFBTSx1Q0FBdUMsQ0FBQztBQUM3RSxPQUFPLEVBQUMsc0JBQXNCLEVBQUMsTUFBTSxrQkFBa0IsQ0FBQztBQUV4RCxPQUFPLEVBQUMsVUFBVSxFQUFFLFFBQVEsRUFBQyxNQUFNLDBCQUEwQixDQUFDO1dBSVEsc0JBQXNCOztNQUR0RixTQUFTLEdBQ1gsc0JBQXNCLENBQWdCLEVBQUMsT0FBTyxFQUFFLE1BQU0sRUFBRSxRQUFRLElBQXdCLEVBQUMsQ0FBQzs7TUFDeEYsV0FBVyxHQUFVLEVBQUU7Ozs7OztBQUU3QixNQUFNLFVBQVUsa0NBQWtDLENBQzlDLElBQWUsRUFBRSxRQUNvRDtJQUN2RSxJQUFJLENBQUMsUUFBUSxFQUFFOztjQUNQLHNCQUFzQixHQUFHLElBQUksc0JBQXNCLEVBQUU7O2NBQ3JELElBQUksR0FBRyxzQkFBc0IsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDO1FBQ3BELDJCQUEyQjtRQUMzQjs7O1FBQU8sR0FBRyxFQUFFLENBQUMsSUFBSSxJQUFJLENBQUMsR0FBRyxVQUFVLENBQUMsbUJBQUEsSUFBSSxFQUFTLENBQUMsQ0FBQyxFQUFDO0tBQ3JEO0lBRUQsSUFBSSxTQUFTLElBQUksUUFBUSxFQUFFOztjQUNuQixhQUFhLEdBQUcsQ0FBQyxtQkFBQSxRQUFRLEVBQXFCLENBQUM7UUFDckQ7OztRQUFPLEdBQUcsRUFBRSxDQUFDLGFBQWEsQ0FBQyxRQUFRLEVBQUM7S0FDckM7U0FBTSxJQUFJLENBQUMsbUJBQUEsUUFBUSxFQUF3QixDQUFDLENBQUMsV0FBVyxFQUFFOztjQUNuRCxnQkFBZ0IsR0FBRyxDQUFDLG1CQUFBLFFBQVEsRUFBd0IsQ0FBQztRQUMzRDs7O1FBQU8sR0FBRyxFQUFFLENBQUMsUUFBUSxDQUFDLGdCQUFnQixDQUFDLFdBQVcsQ0FBQyxFQUFDO0tBQ3JEO1NBQU0sSUFBSSxDQUFDLG1CQUFBLFFBQVEsRUFBdUIsQ0FBQyxDQUFDLFVBQVUsRUFBRTs7Y0FDakQsZUFBZSxHQUFHLENBQUMsbUJBQUEsUUFBUSxFQUF1QixDQUFDO1FBQ3pEOzs7UUFBTyxHQUFHLEVBQUUsQ0FBQyxlQUFlLENBQUMsVUFBVSxDQUFDLEdBQUcsVUFBVSxDQUFDLGVBQWUsQ0FBQyxJQUFJLElBQUksV0FBVyxDQUFDLENBQUMsRUFBQztLQUM3RjtTQUFNLElBQUksQ0FBQyxtQkFBQSxRQUFRLEVBQStDLENBQUMsQ0FBQyxRQUFRLEVBQUU7O2NBQ3ZFLGFBQWEsR0FBRyxDQUFDLG1CQUFBLFFBQVEsRUFBK0MsQ0FBQzs7WUFDM0UsSUFBSSxHQUFHLENBQUMsbUJBQUEsUUFBUSxFQUEyQixDQUFDLENBQUMsSUFBSTtRQUNyRCxJQUFJLENBQUMsSUFBSSxFQUFFOztrQkFDSCxzQkFBc0IsR0FBRyxJQUFJLHNCQUFzQixFQUFFO1lBQzNELElBQUksR0FBRyxzQkFBc0IsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDaEQ7UUFDRDs7O1FBQU8sR0FBRyxFQUFFLENBQUMsSUFBSSxhQUFhLENBQUMsUUFBUSxDQUFDLEdBQUcsVUFBVSxDQUFDLElBQUksQ0FBQyxDQUFDLEVBQUM7S0FDOUQ7U0FBTTs7WUFDRCxJQUFJLEdBQUcsQ0FBQyxtQkFBQSxRQUFRLEVBQTJCLENBQUMsQ0FBQyxJQUFJO1FBQ3JELElBQUksQ0FBQyxJQUFJLEVBQUU7O2tCQUNILHNCQUFzQixHQUFHLElBQUksc0JBQXNCLEVBQUU7WUFDM0QsSUFBSSxHQUFHLHNCQUFzQixDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUNoRDtRQUNEOzs7UUFBTyxHQUFHLEVBQUUsQ0FBQyxJQUFJLElBQUksQ0FBQyxHQUFHLFVBQVUsQ0FBQyxtQkFBQSxJQUFJLEVBQUUsQ0FBQyxDQUFDLEVBQUM7S0FDOUM7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7UmVmbGVjdGlvbkNhcGFiaWxpdGllc30gZnJvbSAnLi4vcmVmbGVjdGlvbi9yZWZsZWN0aW9uX2NhcGFiaWxpdGllcyc7XG5pbXBvcnQge2dldENsb3N1cmVTYWZlUHJvcGVydHl9IGZyb20gJy4uL3V0aWwvcHJvcGVydHknO1xuXG5pbXBvcnQge2luamVjdEFyZ3MsIMm1ybVpbmplY3R9IGZyb20gJy4vaW5qZWN0b3JfY29tcGF0aWJpbGl0eSc7XG5pbXBvcnQge0NsYXNzU2Fuc1Byb3ZpZGVyLCBDb25zdHJ1Y3RvclNhbnNQcm92aWRlciwgRXhpc3RpbmdTYW5zUHJvdmlkZXIsIEZhY3RvcnlTYW5zUHJvdmlkZXIsIFN0YXRpY0NsYXNzU2Fuc1Byb3ZpZGVyLCBWYWx1ZVByb3ZpZGVyLCBWYWx1ZVNhbnNQcm92aWRlcn0gZnJvbSAnLi9pbnRlcmZhY2UvcHJvdmlkZXInO1xuXG5jb25zdCBVU0VfVkFMVUUgPVxuICAgIGdldENsb3N1cmVTYWZlUHJvcGVydHk8VmFsdWVQcm92aWRlcj4oe3Byb3ZpZGU6IFN0cmluZywgdXNlVmFsdWU6IGdldENsb3N1cmVTYWZlUHJvcGVydHl9KTtcbmNvbnN0IEVNUFRZX0FSUkFZOiBhbnlbXSA9IFtdO1xuXG5leHBvcnQgZnVuY3Rpb24gY29udmVydEluamVjdGFibGVQcm92aWRlclRvRmFjdG9yeShcbiAgICB0eXBlOiBUeXBlPGFueT4sIHByb3ZpZGVyPzogVmFsdWVTYW5zUHJvdmlkZXIgfCBFeGlzdGluZ1NhbnNQcm92aWRlciB8IFN0YXRpY0NsYXNzU2Fuc1Byb3ZpZGVyIHxcbiAgICAgICAgQ29uc3RydWN0b3JTYW5zUHJvdmlkZXIgfCBGYWN0b3J5U2Fuc1Byb3ZpZGVyIHwgQ2xhc3NTYW5zUHJvdmlkZXIpOiAoKSA9PiBhbnkge1xuICBpZiAoIXByb3ZpZGVyKSB7XG4gICAgY29uc3QgcmVmbGVjdGlvbkNhcGFiaWxpdGllcyA9IG5ldyBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzKCk7XG4gICAgY29uc3QgZGVwcyA9IHJlZmxlY3Rpb25DYXBhYmlsaXRpZXMucGFyYW1ldGVycyh0eXBlKTtcbiAgICAvLyBUT0RPIC0gY29udmVydCB0byBmbGFncy5cbiAgICByZXR1cm4gKCkgPT4gbmV3IHR5cGUoLi4uaW5qZWN0QXJncyhkZXBzIGFzIGFueVtdKSk7XG4gIH1cblxuICBpZiAoVVNFX1ZBTFVFIGluIHByb3ZpZGVyKSB7XG4gICAgY29uc3QgdmFsdWVQcm92aWRlciA9IChwcm92aWRlciBhcyBWYWx1ZVNhbnNQcm92aWRlcik7XG4gICAgcmV0dXJuICgpID0+IHZhbHVlUHJvdmlkZXIudXNlVmFsdWU7XG4gIH0gZWxzZSBpZiAoKHByb3ZpZGVyIGFzIEV4aXN0aW5nU2Fuc1Byb3ZpZGVyKS51c2VFeGlzdGluZykge1xuICAgIGNvbnN0IGV4aXN0aW5nUHJvdmlkZXIgPSAocHJvdmlkZXIgYXMgRXhpc3RpbmdTYW5zUHJvdmlkZXIpO1xuICAgIHJldHVybiAoKSA9PiDJtcm1aW5qZWN0KGV4aXN0aW5nUHJvdmlkZXIudXNlRXhpc3RpbmcpO1xuICB9IGVsc2UgaWYgKChwcm92aWRlciBhcyBGYWN0b3J5U2Fuc1Byb3ZpZGVyKS51c2VGYWN0b3J5KSB7XG4gICAgY29uc3QgZmFjdG9yeVByb3ZpZGVyID0gKHByb3ZpZGVyIGFzIEZhY3RvcnlTYW5zUHJvdmlkZXIpO1xuICAgIHJldHVybiAoKSA9PiBmYWN0b3J5UHJvdmlkZXIudXNlRmFjdG9yeSguLi5pbmplY3RBcmdzKGZhY3RvcnlQcm92aWRlci5kZXBzIHx8IEVNUFRZX0FSUkFZKSk7XG4gIH0gZWxzZSBpZiAoKHByb3ZpZGVyIGFzIFN0YXRpY0NsYXNzU2Fuc1Byb3ZpZGVyIHwgQ2xhc3NTYW5zUHJvdmlkZXIpLnVzZUNsYXNzKSB7XG4gICAgY29uc3QgY2xhc3NQcm92aWRlciA9IChwcm92aWRlciBhcyBTdGF0aWNDbGFzc1NhbnNQcm92aWRlciB8IENsYXNzU2Fuc1Byb3ZpZGVyKTtcbiAgICBsZXQgZGVwcyA9IChwcm92aWRlciBhcyBTdGF0aWNDbGFzc1NhbnNQcm92aWRlcikuZGVwcztcbiAgICBpZiAoIWRlcHMpIHtcbiAgICAgIGNvbnN0IHJlZmxlY3Rpb25DYXBhYmlsaXRpZXMgPSBuZXcgUmVmbGVjdGlvbkNhcGFiaWxpdGllcygpO1xuICAgICAgZGVwcyA9IHJlZmxlY3Rpb25DYXBhYmlsaXRpZXMucGFyYW1ldGVycyh0eXBlKTtcbiAgICB9XG4gICAgcmV0dXJuICgpID0+IG5ldyBjbGFzc1Byb3ZpZGVyLnVzZUNsYXNzKC4uLmluamVjdEFyZ3MoZGVwcykpO1xuICB9IGVsc2Uge1xuICAgIGxldCBkZXBzID0gKHByb3ZpZGVyIGFzIENvbnN0cnVjdG9yU2Fuc1Byb3ZpZGVyKS5kZXBzO1xuICAgIGlmICghZGVwcykge1xuICAgICAgY29uc3QgcmVmbGVjdGlvbkNhcGFiaWxpdGllcyA9IG5ldyBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzKCk7XG4gICAgICBkZXBzID0gcmVmbGVjdGlvbkNhcGFiaWxpdGllcy5wYXJhbWV0ZXJzKHR5cGUpO1xuICAgIH1cbiAgICByZXR1cm4gKCkgPT4gbmV3IHR5cGUoLi4uaW5qZWN0QXJncyhkZXBzICEpKTtcbiAgfVxufVxuIl19