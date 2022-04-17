/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ChangeDetectorRef } from '../../change_detection/change_detector_ref';
import { getCompilerFacade } from '../../compiler/compiler_facade';
import { ReflectionCapabilities } from '../../reflection/reflection_capabilities';
import { Attribute, Host, Inject, Optional, Self, SkipSelf } from '../metadata';
var _reflect = null;
export function getReflect() {
    return (_reflect = _reflect || new ReflectionCapabilities());
}
export function reflectDependencies(type) {
    return convertDependencies(getReflect().parameters(type));
}
export function convertDependencies(deps) {
    var compiler = getCompilerFacade();
    return deps.map(function (dep) { return reflectDependency(compiler, dep); });
}
function reflectDependency(compiler, dep) {
    var meta = {
        token: null,
        host: false,
        optional: false,
        resolved: compiler.R3ResolvedDependencyType.Token,
        self: false,
        skipSelf: false,
    };
    function setTokenAndResolvedType(token) {
        meta.resolved = compiler.R3ResolvedDependencyType.Token;
        meta.token = token;
    }
    if (Array.isArray(dep)) {
        if (dep.length === 0) {
            throw new Error('Dependency array must have arguments.');
        }
        for (var j = 0; j < dep.length; j++) {
            var param = dep[j];
            if (param === undefined) {
                // param may be undefined if type of dep is not set by ngtsc
                continue;
            }
            else if (param instanceof Optional || param.__proto__.ngMetadataName === 'Optional') {
                meta.optional = true;
            }
            else if (param instanceof SkipSelf || param.__proto__.ngMetadataName === 'SkipSelf') {
                meta.skipSelf = true;
            }
            else if (param instanceof Self || param.__proto__.ngMetadataName === 'Self') {
                meta.self = true;
            }
            else if (param instanceof Host || param.__proto__.ngMetadataName === 'Host') {
                meta.host = true;
            }
            else if (param instanceof Inject) {
                meta.token = param.token;
            }
            else if (param instanceof Attribute) {
                if (param.attributeName === undefined) {
                    throw new Error("Attribute name must be defined.");
                }
                meta.token = param.attributeName;
                meta.resolved = compiler.R3ResolvedDependencyType.Attribute;
            }
            else if (param === ChangeDetectorRef) {
                meta.token = param;
                meta.resolved = compiler.R3ResolvedDependencyType.ChangeDetectorRef;
            }
            else {
                setTokenAndResolvedType(param);
            }
        }
    }
    else {
        setTokenAndResolvedType(dep);
    }
    return meta;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2RpL2ppdC91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLDRDQUE0QyxDQUFDO0FBQzdFLE9BQU8sRUFBNkMsaUJBQWlCLEVBQUMsTUFBTSxnQ0FBZ0MsQ0FBQztBQUU3RyxPQUFPLEVBQUMsc0JBQXNCLEVBQUMsTUFBTSwwQ0FBMEMsQ0FBQztBQUNoRixPQUFPLEVBQUMsU0FBUyxFQUFFLElBQUksRUFBRSxNQUFNLEVBQUUsUUFBUSxFQUFFLElBQUksRUFBRSxRQUFRLEVBQUMsTUFBTSxhQUFhLENBQUM7QUFFOUUsSUFBSSxRQUFRLEdBQWdDLElBQUksQ0FBQztBQUVqRCxNQUFNLFVBQVUsVUFBVTtJQUN4QixPQUFPLENBQUMsUUFBUSxHQUFHLFFBQVEsSUFBSSxJQUFJLHNCQUFzQixFQUFFLENBQUMsQ0FBQztBQUMvRCxDQUFDO0FBRUQsTUFBTSxVQUFVLG1CQUFtQixDQUFDLElBQWU7SUFDakQsT0FBTyxtQkFBbUIsQ0FBQyxVQUFVLEVBQUUsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztBQUM1RCxDQUFDO0FBRUQsTUFBTSxVQUFVLG1CQUFtQixDQUFDLElBQVc7SUFDN0MsSUFBTSxRQUFRLEdBQUcsaUJBQWlCLEVBQUUsQ0FBQztJQUNyQyxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsVUFBQSxHQUFHLElBQUksT0FBQSxpQkFBaUIsQ0FBQyxRQUFRLEVBQUUsR0FBRyxDQUFDLEVBQWhDLENBQWdDLENBQUMsQ0FBQztBQUMzRCxDQUFDO0FBRUQsU0FBUyxpQkFBaUIsQ0FBQyxRQUF3QixFQUFFLEdBQWdCO0lBQ25FLElBQU0sSUFBSSxHQUErQjtRQUN2QyxLQUFLLEVBQUUsSUFBSTtRQUNYLElBQUksRUFBRSxLQUFLO1FBQ1gsUUFBUSxFQUFFLEtBQUs7UUFDZixRQUFRLEVBQUUsUUFBUSxDQUFDLHdCQUF3QixDQUFDLEtBQUs7UUFDakQsSUFBSSxFQUFFLEtBQUs7UUFDWCxRQUFRLEVBQUUsS0FBSztLQUNoQixDQUFDO0lBRUYsU0FBUyx1QkFBdUIsQ0FBQyxLQUFVO1FBQ3pDLElBQUksQ0FBQyxRQUFRLEdBQUcsUUFBUSxDQUFDLHdCQUF3QixDQUFDLEtBQUssQ0FBQztRQUN4RCxJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztJQUNyQixDQUFDO0lBRUQsSUFBSSxLQUFLLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFO1FBQ3RCLElBQUksR0FBRyxDQUFDLE1BQU0sS0FBSyxDQUFDLEVBQUU7WUFDcEIsTUFBTSxJQUFJLEtBQUssQ0FBQyx1Q0FBdUMsQ0FBQyxDQUFDO1NBQzFEO1FBQ0QsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLEdBQUcsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7WUFDbkMsSUFBTSxLQUFLLEdBQUcsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDO1lBQ3JCLElBQUksS0FBSyxLQUFLLFNBQVMsRUFBRTtnQkFDdkIsNERBQTREO2dCQUM1RCxTQUFTO2FBQ1Y7aUJBQU0sSUFBSSxLQUFLLFlBQVksUUFBUSxJQUFJLEtBQUssQ0FBQyxTQUFTLENBQUMsY0FBYyxLQUFLLFVBQVUsRUFBRTtnQkFDckYsSUFBSSxDQUFDLFFBQVEsR0FBRyxJQUFJLENBQUM7YUFDdEI7aUJBQU0sSUFBSSxLQUFLLFlBQVksUUFBUSxJQUFJLEtBQUssQ0FBQyxTQUFTLENBQUMsY0FBYyxLQUFLLFVBQVUsRUFBRTtnQkFDckYsSUFBSSxDQUFDLFFBQVEsR0FBRyxJQUFJLENBQUM7YUFDdEI7aUJBQU0sSUFBSSxLQUFLLFlBQVksSUFBSSxJQUFJLEtBQUssQ0FBQyxTQUFTLENBQUMsY0FBYyxLQUFLLE1BQU0sRUFBRTtnQkFDN0UsSUFBSSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUM7YUFDbEI7aUJBQU0sSUFBSSxLQUFLLFlBQVksSUFBSSxJQUFJLEtBQUssQ0FBQyxTQUFTLENBQUMsY0FBYyxLQUFLLE1BQU0sRUFBRTtnQkFDN0UsSUFBSSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUM7YUFDbEI7aUJBQU0sSUFBSSxLQUFLLFlBQVksTUFBTSxFQUFFO2dCQUNsQyxJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUM7YUFDMUI7aUJBQU0sSUFBSSxLQUFLLFlBQVksU0FBUyxFQUFFO2dCQUNyQyxJQUFJLEtBQUssQ0FBQyxhQUFhLEtBQUssU0FBUyxFQUFFO29CQUNyQyxNQUFNLElBQUksS0FBSyxDQUFDLGlDQUFpQyxDQUFDLENBQUM7aUJBQ3BEO2dCQUNELElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDLGFBQWEsQ0FBQztnQkFDakMsSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsd0JBQXdCLENBQUMsU0FBUyxDQUFDO2FBQzdEO2lCQUFNLElBQUksS0FBSyxLQUFLLGlCQUFpQixFQUFFO2dCQUN0QyxJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztnQkFDbkIsSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsd0JBQXdCLENBQUMsaUJBQWlCLENBQUM7YUFDckU7aUJBQU07Z0JBQ0wsdUJBQXVCLENBQUMsS0FBSyxDQUFDLENBQUM7YUFDaEM7U0FDRjtLQUNGO1NBQU07UUFDTCx1QkFBdUIsQ0FBQyxHQUFHLENBQUMsQ0FBQztLQUM5QjtJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDaGFuZ2VEZXRlY3RvclJlZn0gZnJvbSAnLi4vLi4vY2hhbmdlX2RldGVjdGlvbi9jaGFuZ2VfZGV0ZWN0b3JfcmVmJztcbmltcG9ydCB7Q29tcGlsZXJGYWNhZGUsIFIzRGVwZW5kZW5jeU1ldGFkYXRhRmFjYWRlLCBnZXRDb21waWxlckZhY2FkZX0gZnJvbSAnLi4vLi4vY29tcGlsZXIvY29tcGlsZXJfZmFjYWRlJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzfSBmcm9tICcuLi8uLi9yZWZsZWN0aW9uL3JlZmxlY3Rpb25fY2FwYWJpbGl0aWVzJztcbmltcG9ydCB7QXR0cmlidXRlLCBIb3N0LCBJbmplY3QsIE9wdGlvbmFsLCBTZWxmLCBTa2lwU2VsZn0gZnJvbSAnLi4vbWV0YWRhdGEnO1xuXG5sZXQgX3JlZmxlY3Q6IFJlZmxlY3Rpb25DYXBhYmlsaXRpZXN8bnVsbCA9IG51bGw7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZsZWN0KCk6IFJlZmxlY3Rpb25DYXBhYmlsaXRpZXMge1xuICByZXR1cm4gKF9yZWZsZWN0ID0gX3JlZmxlY3QgfHwgbmV3IFJlZmxlY3Rpb25DYXBhYmlsaXRpZXMoKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZWZsZWN0RGVwZW5kZW5jaWVzKHR5cGU6IFR5cGU8YW55Pik6IFIzRGVwZW5kZW5jeU1ldGFkYXRhRmFjYWRlW10ge1xuICByZXR1cm4gY29udmVydERlcGVuZGVuY2llcyhnZXRSZWZsZWN0KCkucGFyYW1ldGVycyh0eXBlKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjb252ZXJ0RGVwZW5kZW5jaWVzKGRlcHM6IGFueVtdKTogUjNEZXBlbmRlbmN5TWV0YWRhdGFGYWNhZGVbXSB7XG4gIGNvbnN0IGNvbXBpbGVyID0gZ2V0Q29tcGlsZXJGYWNhZGUoKTtcbiAgcmV0dXJuIGRlcHMubWFwKGRlcCA9PiByZWZsZWN0RGVwZW5kZW5jeShjb21waWxlciwgZGVwKSk7XG59XG5cbmZ1bmN0aW9uIHJlZmxlY3REZXBlbmRlbmN5KGNvbXBpbGVyOiBDb21waWxlckZhY2FkZSwgZGVwOiBhbnkgfCBhbnlbXSk6IFIzRGVwZW5kZW5jeU1ldGFkYXRhRmFjYWRlIHtcbiAgY29uc3QgbWV0YTogUjNEZXBlbmRlbmN5TWV0YWRhdGFGYWNhZGUgPSB7XG4gICAgdG9rZW46IG51bGwsXG4gICAgaG9zdDogZmFsc2UsXG4gICAgb3B0aW9uYWw6IGZhbHNlLFxuICAgIHJlc29sdmVkOiBjb21waWxlci5SM1Jlc29sdmVkRGVwZW5kZW5jeVR5cGUuVG9rZW4sXG4gICAgc2VsZjogZmFsc2UsXG4gICAgc2tpcFNlbGY6IGZhbHNlLFxuICB9O1xuXG4gIGZ1bmN0aW9uIHNldFRva2VuQW5kUmVzb2x2ZWRUeXBlKHRva2VuOiBhbnkpOiB2b2lkIHtcbiAgICBtZXRhLnJlc29sdmVkID0gY29tcGlsZXIuUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlLlRva2VuO1xuICAgIG1ldGEudG9rZW4gPSB0b2tlbjtcbiAgfVxuXG4gIGlmIChBcnJheS5pc0FycmF5KGRlcCkpIHtcbiAgICBpZiAoZGVwLmxlbmd0aCA9PT0gMCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdEZXBlbmRlbmN5IGFycmF5IG11c3QgaGF2ZSBhcmd1bWVudHMuJyk7XG4gICAgfVxuICAgIGZvciAobGV0IGogPSAwOyBqIDwgZGVwLmxlbmd0aDsgaisrKSB7XG4gICAgICBjb25zdCBwYXJhbSA9IGRlcFtqXTtcbiAgICAgIGlmIChwYXJhbSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgIC8vIHBhcmFtIG1heSBiZSB1bmRlZmluZWQgaWYgdHlwZSBvZiBkZXAgaXMgbm90IHNldCBieSBuZ3RzY1xuICAgICAgICBjb250aW51ZTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gaW5zdGFuY2VvZiBPcHRpb25hbCB8fCBwYXJhbS5fX3Byb3RvX18ubmdNZXRhZGF0YU5hbWUgPT09ICdPcHRpb25hbCcpIHtcbiAgICAgICAgbWV0YS5vcHRpb25hbCA9IHRydWU7XG4gICAgICB9IGVsc2UgaWYgKHBhcmFtIGluc3RhbmNlb2YgU2tpcFNlbGYgfHwgcGFyYW0uX19wcm90b19fLm5nTWV0YWRhdGFOYW1lID09PSAnU2tpcFNlbGYnKSB7XG4gICAgICAgIG1ldGEuc2tpcFNlbGYgPSB0cnVlO1xuICAgICAgfSBlbHNlIGlmIChwYXJhbSBpbnN0YW5jZW9mIFNlbGYgfHwgcGFyYW0uX19wcm90b19fLm5nTWV0YWRhdGFOYW1lID09PSAnU2VsZicpIHtcbiAgICAgICAgbWV0YS5zZWxmID0gdHJ1ZTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gaW5zdGFuY2VvZiBIb3N0IHx8IHBhcmFtLl9fcHJvdG9fXy5uZ01ldGFkYXRhTmFtZSA9PT0gJ0hvc3QnKSB7XG4gICAgICAgIG1ldGEuaG9zdCA9IHRydWU7XG4gICAgICB9IGVsc2UgaWYgKHBhcmFtIGluc3RhbmNlb2YgSW5qZWN0KSB7XG4gICAgICAgIG1ldGEudG9rZW4gPSBwYXJhbS50b2tlbjtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gaW5zdGFuY2VvZiBBdHRyaWJ1dGUpIHtcbiAgICAgICAgaWYgKHBhcmFtLmF0dHJpYnV0ZU5hbWUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgQXR0cmlidXRlIG5hbWUgbXVzdCBiZSBkZWZpbmVkLmApO1xuICAgICAgICB9XG4gICAgICAgIG1ldGEudG9rZW4gPSBwYXJhbS5hdHRyaWJ1dGVOYW1lO1xuICAgICAgICBtZXRhLnJlc29sdmVkID0gY29tcGlsZXIuUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlLkF0dHJpYnV0ZTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gPT09IENoYW5nZURldGVjdG9yUmVmKSB7XG4gICAgICAgIG1ldGEudG9rZW4gPSBwYXJhbTtcbiAgICAgICAgbWV0YS5yZXNvbHZlZCA9IGNvbXBpbGVyLlIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5DaGFuZ2VEZXRlY3RvclJlZjtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHNldFRva2VuQW5kUmVzb2x2ZWRUeXBlKHBhcmFtKTtcbiAgICAgIH1cbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgc2V0VG9rZW5BbmRSZXNvbHZlZFR5cGUoZGVwKTtcbiAgfVxuICByZXR1cm4gbWV0YTtcbn1cbiJdfQ==