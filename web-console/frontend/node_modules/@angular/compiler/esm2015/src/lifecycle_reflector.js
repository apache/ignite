/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
export var LifecycleHooks;
(function (LifecycleHooks) {
    LifecycleHooks[LifecycleHooks["OnInit"] = 0] = "OnInit";
    LifecycleHooks[LifecycleHooks["OnDestroy"] = 1] = "OnDestroy";
    LifecycleHooks[LifecycleHooks["DoCheck"] = 2] = "DoCheck";
    LifecycleHooks[LifecycleHooks["OnChanges"] = 3] = "OnChanges";
    LifecycleHooks[LifecycleHooks["AfterContentInit"] = 4] = "AfterContentInit";
    LifecycleHooks[LifecycleHooks["AfterContentChecked"] = 5] = "AfterContentChecked";
    LifecycleHooks[LifecycleHooks["AfterViewInit"] = 6] = "AfterViewInit";
    LifecycleHooks[LifecycleHooks["AfterViewChecked"] = 7] = "AfterViewChecked";
})(LifecycleHooks || (LifecycleHooks = {}));
export const LIFECYCLE_HOOKS_VALUES = [
    LifecycleHooks.OnInit, LifecycleHooks.OnDestroy, LifecycleHooks.DoCheck, LifecycleHooks.OnChanges,
    LifecycleHooks.AfterContentInit, LifecycleHooks.AfterContentChecked, LifecycleHooks.AfterViewInit,
    LifecycleHooks.AfterViewChecked
];
export function hasLifecycleHook(reflector, hook, token) {
    return reflector.hasLifecycleHook(token, getHookName(hook));
}
export function getAllLifecycleHooks(reflector, token) {
    return LIFECYCLE_HOOKS_VALUES.filter(hook => hasLifecycleHook(reflector, hook, token));
}
function getHookName(hook) {
    switch (hook) {
        case LifecycleHooks.OnInit:
            return 'ngOnInit';
        case LifecycleHooks.OnDestroy:
            return 'ngOnDestroy';
        case LifecycleHooks.DoCheck:
            return 'ngDoCheck';
        case LifecycleHooks.OnChanges:
            return 'ngOnChanges';
        case LifecycleHooks.AfterContentInit:
            return 'ngAfterContentInit';
        case LifecycleHooks.AfterContentChecked:
            return 'ngAfterContentChecked';
        case LifecycleHooks.AfterViewInit:
            return 'ngAfterViewInit';
        case LifecycleHooks.AfterViewChecked:
            return 'ngAfterViewChecked';
        default:
            // This default case is not needed by TypeScript compiler, as the switch is exhaustive.
            // However Closure Compiler does not understand that and reports an error in typed mode.
            // The `throw new Error` below works around the problem, and the unexpected: never variable
            // makes sure tsc still checks this code is unreachable.
            const unexpected = hook;
            throw new Error(`unexpected ${unexpected}`);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGlmZWN5Y2xlX3JlZmxlY3Rvci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9saWZlY3ljbGVfcmVmbGVjdG9yLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUlILE1BQU0sQ0FBTixJQUFZLGNBU1g7QUFURCxXQUFZLGNBQWM7SUFDeEIsdURBQU0sQ0FBQTtJQUNOLDZEQUFTLENBQUE7SUFDVCx5REFBTyxDQUFBO0lBQ1AsNkRBQVMsQ0FBQTtJQUNULDJFQUFnQixDQUFBO0lBQ2hCLGlGQUFtQixDQUFBO0lBQ25CLHFFQUFhLENBQUE7SUFDYiwyRUFBZ0IsQ0FBQTtBQUNsQixDQUFDLEVBVFcsY0FBYyxLQUFkLGNBQWMsUUFTekI7QUFFRCxNQUFNLENBQUMsTUFBTSxzQkFBc0IsR0FBRztJQUNwQyxjQUFjLENBQUMsTUFBTSxFQUFFLGNBQWMsQ0FBQyxTQUFTLEVBQUUsY0FBYyxDQUFDLE9BQU8sRUFBRSxjQUFjLENBQUMsU0FBUztJQUNqRyxjQUFjLENBQUMsZ0JBQWdCLEVBQUUsY0FBYyxDQUFDLG1CQUFtQixFQUFFLGNBQWMsQ0FBQyxhQUFhO0lBQ2pHLGNBQWMsQ0FBQyxnQkFBZ0I7Q0FDaEMsQ0FBQztBQUVGLE1BQU0sVUFBVSxnQkFBZ0IsQ0FDNUIsU0FBMkIsRUFBRSxJQUFvQixFQUFFLEtBQVU7SUFDL0QsT0FBTyxTQUFTLENBQUMsZ0JBQWdCLENBQUMsS0FBSyxFQUFFLFdBQVcsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO0FBQzlELENBQUM7QUFFRCxNQUFNLFVBQVUsb0JBQW9CLENBQUMsU0FBMkIsRUFBRSxLQUFVO0lBQzFFLE9BQU8sc0JBQXNCLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsZ0JBQWdCLENBQUMsU0FBUyxFQUFFLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDO0FBQ3pGLENBQUM7QUFFRCxTQUFTLFdBQVcsQ0FBQyxJQUFvQjtJQUN2QyxRQUFRLElBQUksRUFBRTtRQUNaLEtBQUssY0FBYyxDQUFDLE1BQU07WUFDeEIsT0FBTyxVQUFVLENBQUM7UUFDcEIsS0FBSyxjQUFjLENBQUMsU0FBUztZQUMzQixPQUFPLGFBQWEsQ0FBQztRQUN2QixLQUFLLGNBQWMsQ0FBQyxPQUFPO1lBQ3pCLE9BQU8sV0FBVyxDQUFDO1FBQ3JCLEtBQUssY0FBYyxDQUFDLFNBQVM7WUFDM0IsT0FBTyxhQUFhLENBQUM7UUFDdkIsS0FBSyxjQUFjLENBQUMsZ0JBQWdCO1lBQ2xDLE9BQU8sb0JBQW9CLENBQUM7UUFDOUIsS0FBSyxjQUFjLENBQUMsbUJBQW1CO1lBQ3JDLE9BQU8sdUJBQXVCLENBQUM7UUFDakMsS0FBSyxjQUFjLENBQUMsYUFBYTtZQUMvQixPQUFPLGlCQUFpQixDQUFDO1FBQzNCLEtBQUssY0FBYyxDQUFDLGdCQUFnQjtZQUNsQyxPQUFPLG9CQUFvQixDQUFDO1FBQzlCO1lBQ0UsdUZBQXVGO1lBQ3ZGLHdGQUF3RjtZQUN4RiwyRkFBMkY7WUFDM0Ysd0RBQXdEO1lBQ3hELE1BQU0sVUFBVSxHQUFVLElBQUksQ0FBQztZQUMvQixNQUFNLElBQUksS0FBSyxDQUFDLGNBQWMsVUFBVSxFQUFFLENBQUMsQ0FBQztLQUMvQztBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZVJlZmxlY3Rvcn0gZnJvbSAnLi9jb21waWxlX3JlZmxlY3Rvcic7XG5cbmV4cG9ydCBlbnVtIExpZmVjeWNsZUhvb2tzIHtcbiAgT25Jbml0LFxuICBPbkRlc3Ryb3ksXG4gIERvQ2hlY2ssXG4gIE9uQ2hhbmdlcyxcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQWZ0ZXJDb250ZW50Q2hlY2tlZCxcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQWZ0ZXJWaWV3Q2hlY2tlZFxufVxuXG5leHBvcnQgY29uc3QgTElGRUNZQ0xFX0hPT0tTX1ZBTFVFUyA9IFtcbiAgTGlmZWN5Y2xlSG9va3MuT25Jbml0LCBMaWZlY3ljbGVIb29rcy5PbkRlc3Ryb3ksIExpZmVjeWNsZUhvb2tzLkRvQ2hlY2ssIExpZmVjeWNsZUhvb2tzLk9uQ2hhbmdlcyxcbiAgTGlmZWN5Y2xlSG9va3MuQWZ0ZXJDb250ZW50SW5pdCwgTGlmZWN5Y2xlSG9va3MuQWZ0ZXJDb250ZW50Q2hlY2tlZCwgTGlmZWN5Y2xlSG9va3MuQWZ0ZXJWaWV3SW5pdCxcbiAgTGlmZWN5Y2xlSG9va3MuQWZ0ZXJWaWV3Q2hlY2tlZFxuXTtcblxuZXhwb3J0IGZ1bmN0aW9uIGhhc0xpZmVjeWNsZUhvb2soXG4gICAgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLCBob29rOiBMaWZlY3ljbGVIb29rcywgdG9rZW46IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gcmVmbGVjdG9yLmhhc0xpZmVjeWNsZUhvb2sodG9rZW4sIGdldEhvb2tOYW1lKGhvb2spKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbExpZmVjeWNsZUhvb2tzKHJlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3RvciwgdG9rZW46IGFueSk6IExpZmVjeWNsZUhvb2tzW10ge1xuICByZXR1cm4gTElGRUNZQ0xFX0hPT0tTX1ZBTFVFUy5maWx0ZXIoaG9vayA9PiBoYXNMaWZlY3ljbGVIb29rKHJlZmxlY3RvciwgaG9vaywgdG9rZW4pKTtcbn1cblxuZnVuY3Rpb24gZ2V0SG9va05hbWUoaG9vazogTGlmZWN5Y2xlSG9va3MpOiBzdHJpbmcge1xuICBzd2l0Y2ggKGhvb2spIHtcbiAgICBjYXNlIExpZmVjeWNsZUhvb2tzLk9uSW5pdDpcbiAgICAgIHJldHVybiAnbmdPbkluaXQnO1xuICAgIGNhc2UgTGlmZWN5Y2xlSG9va3MuT25EZXN0cm95OlxuICAgICAgcmV0dXJuICduZ09uRGVzdHJveSc7XG4gICAgY2FzZSBMaWZlY3ljbGVIb29rcy5Eb0NoZWNrOlxuICAgICAgcmV0dXJuICduZ0RvQ2hlY2snO1xuICAgIGNhc2UgTGlmZWN5Y2xlSG9va3MuT25DaGFuZ2VzOlxuICAgICAgcmV0dXJuICduZ09uQ2hhbmdlcyc7XG4gICAgY2FzZSBMaWZlY3ljbGVIb29rcy5BZnRlckNvbnRlbnRJbml0OlxuICAgICAgcmV0dXJuICduZ0FmdGVyQ29udGVudEluaXQnO1xuICAgIGNhc2UgTGlmZWN5Y2xlSG9va3MuQWZ0ZXJDb250ZW50Q2hlY2tlZDpcbiAgICAgIHJldHVybiAnbmdBZnRlckNvbnRlbnRDaGVja2VkJztcbiAgICBjYXNlIExpZmVjeWNsZUhvb2tzLkFmdGVyVmlld0luaXQ6XG4gICAgICByZXR1cm4gJ25nQWZ0ZXJWaWV3SW5pdCc7XG4gICAgY2FzZSBMaWZlY3ljbGVIb29rcy5BZnRlclZpZXdDaGVja2VkOlxuICAgICAgcmV0dXJuICduZ0FmdGVyVmlld0NoZWNrZWQnO1xuICAgIGRlZmF1bHQ6XG4gICAgICAvLyBUaGlzIGRlZmF1bHQgY2FzZSBpcyBub3QgbmVlZGVkIGJ5IFR5cGVTY3JpcHQgY29tcGlsZXIsIGFzIHRoZSBzd2l0Y2ggaXMgZXhoYXVzdGl2ZS5cbiAgICAgIC8vIEhvd2V2ZXIgQ2xvc3VyZSBDb21waWxlciBkb2VzIG5vdCB1bmRlcnN0YW5kIHRoYXQgYW5kIHJlcG9ydHMgYW4gZXJyb3IgaW4gdHlwZWQgbW9kZS5cbiAgICAgIC8vIFRoZSBgdGhyb3cgbmV3IEVycm9yYCBiZWxvdyB3b3JrcyBhcm91bmQgdGhlIHByb2JsZW0sIGFuZCB0aGUgdW5leHBlY3RlZDogbmV2ZXIgdmFyaWFibGVcbiAgICAgIC8vIG1ha2VzIHN1cmUgdHNjIHN0aWxsIGNoZWNrcyB0aGlzIGNvZGUgaXMgdW5yZWFjaGFibGUuXG4gICAgICBjb25zdCB1bmV4cGVjdGVkOiBuZXZlciA9IGhvb2s7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHVuZXhwZWN0ZWQgJHt1bmV4cGVjdGVkfWApO1xuICB9XG59XG4iXX0=