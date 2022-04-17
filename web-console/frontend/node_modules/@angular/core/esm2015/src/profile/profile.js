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
import { createScope, detectWTF, endTimeRange, leave, startTimeRange } from './wtf_impl';
/**
 * True if WTF is enabled.
 * @type {?}
 */
export const wtfEnabled = detectWTF();
/**
 * @param {?=} arg0
 * @param {?=} arg1
 * @return {?}
 */
function noopScope(arg0, arg1) {
    return null;
}
/**
 * Create trace scope.
 *
 * Scopes must be strictly nested and are analogous to stack frames, but
 * do not have to follow the stack frames. Instead it is recommended that they follow logical
 * nesting. You may want to use
 * [Event
 * Signatures](http://google.github.io/tracing-framework/instrumenting-code.html#custom-events)
 * as they are defined in WTF.
 *
 * Used to mark scope entry. The return value is used to leave the scope.
 *
 *     var myScope = wtfCreateScope('MyClass#myMethod(ascii someVal)');
 *
 *     someMethod() {
 *        var s = myScope('Foo'); // 'Foo' gets stored in tracing UI
 *        // DO SOME WORK HERE
 *        return wtfLeave(s, 123); // Return value 123
 *     }
 *
 * Note, adding try-finally block around the work to ensure that `wtfLeave` gets called can
 * negatively impact the performance of your application. For this reason we recommend that
 * you don't add them to ensure that `wtfLeave` gets called. In production `wtfLeave` is a noop and
 * so try-finally block has no value. When debugging perf issues, skipping `wtfLeave`, do to
 * exception, will produce incorrect trace, but presence of exception signifies logic error which
 * needs to be fixed before the app should be profiled. Add try-finally only when you expect that
 * an exception is expected during normal execution while profiling.
 *
 * \@publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 * @type {?}
 */
export const wtfCreateScope = wtfEnabled ? createScope : (/**
 * @param {?} signature
 * @param {?=} flags
 * @return {?}
 */
(signature, flags) => noopScope);
/**
 * Used to mark end of Scope.
 *
 * - `scope` to end.
 * - `returnValue` (optional) to be passed to the WTF.
 *
 * Returns the `returnValue for easy chaining.
 * \@publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 * @type {?}
 */
export const wtfLeave = wtfEnabled ? leave : (/**
 * @param {?} s
 * @param {?=} r
 * @return {?}
 */
(s, r) => r);
/**
 * Used to mark Async start. Async are similar to scope but they don't have to be strictly nested.
 * The return value is used in the call to [endAsync]. Async ranges only work if WTF has been
 * enabled.
 *
 *     someMethod() {
 *        var s = wtfStartTimeRange('HTTP:GET', 'some.url');
 *        var future = new Future.delay(5).then((_) {
 *          wtfEndTimeRange(s);
 *        });
 *     }
 * \@publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 * @type {?}
 */
export const wtfStartTimeRange = wtfEnabled ? startTimeRange : (/**
 * @param {?} rangeType
 * @param {?} action
 * @return {?}
 */
(rangeType, action) => null);
/**
 * Ends a async time range operation.
 * [range] is the return value from [wtfStartTimeRange] Async ranges only work if WTF has been
 * enabled.
 * \@publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 * @type {?}
 */
export const wtfEndTimeRange = wtfEnabled ? endTimeRange : (/**
 * @param {?} r
 * @return {?}
 */
(r) => null);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvZmlsZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3Byb2ZpbGUvcHJvZmlsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBYSxXQUFXLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxLQUFLLEVBQUUsY0FBYyxFQUFDLE1BQU0sWUFBWSxDQUFDOzs7OztBQVFuRyxNQUFNLE9BQU8sVUFBVSxHQUFHLFNBQVMsRUFBRTs7Ozs7O0FBRXJDLFNBQVMsU0FBUyxDQUFDLElBQVUsRUFBRSxJQUFVO0lBQ3ZDLE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBaUNELE1BQU0sT0FBTyxjQUFjLEdBQ3ZCLFVBQVUsQ0FBQyxDQUFDLENBQUMsV0FBVyxDQUFDLENBQUM7Ozs7O0FBQUMsQ0FBQyxTQUFpQixFQUFFLEtBQVcsRUFBRSxFQUFFLENBQUMsU0FBUyxDQUFBOzs7Ozs7Ozs7Ozs7QUFZNUUsTUFBTSxPQUFPLFFBQVEsR0FDakIsVUFBVSxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQzs7Ozs7QUFBQyxDQUFDLENBQU0sRUFBRSxDQUFPLEVBQUUsRUFBRSxDQUFDLENBQUMsQ0FBQTs7Ozs7Ozs7Ozs7Ozs7OztBQWdCL0MsTUFBTSxPQUFPLGlCQUFpQixHQUMxQixVQUFVLENBQUMsQ0FBQyxDQUFDLGNBQWMsQ0FBQyxDQUFDOzs7OztBQUFDLENBQUMsU0FBaUIsRUFBRSxNQUFjLEVBQUUsRUFBRSxDQUFDLElBQUksQ0FBQTs7Ozs7Ozs7O0FBUzdFLE1BQU0sT0FBTyxlQUFlLEdBQXlCLFVBQVUsQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLENBQUM7Ozs7QUFBQyxDQUFDLENBQU0sRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFBIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1d0ZlNjb3BlRm4sIGNyZWF0ZVNjb3BlLCBkZXRlY3RXVEYsIGVuZFRpbWVSYW5nZSwgbGVhdmUsIHN0YXJ0VGltZVJhbmdlfSBmcm9tICcuL3d0Zl9pbXBsJztcblxuZXhwb3J0IHtXdGZTY29wZUZufSBmcm9tICcuL3d0Zl9pbXBsJztcblxuXG4vKipcbiAqIFRydWUgaWYgV1RGIGlzIGVuYWJsZWQuXG4gKi9cbmV4cG9ydCBjb25zdCB3dGZFbmFibGVkID0gZGV0ZWN0V1RGKCk7XG5cbmZ1bmN0aW9uIG5vb3BTY29wZShhcmcwPzogYW55LCBhcmcxPzogYW55KTogYW55IHtcbiAgcmV0dXJuIG51bGw7XG59XG5cbi8qKlxuICogQ3JlYXRlIHRyYWNlIHNjb3BlLlxuICpcbiAqIFNjb3BlcyBtdXN0IGJlIHN0cmljdGx5IG5lc3RlZCBhbmQgYXJlIGFuYWxvZ291cyB0byBzdGFjayBmcmFtZXMsIGJ1dFxuICogZG8gbm90IGhhdmUgdG8gZm9sbG93IHRoZSBzdGFjayBmcmFtZXMuIEluc3RlYWQgaXQgaXMgcmVjb21tZW5kZWQgdGhhdCB0aGV5IGZvbGxvdyBsb2dpY2FsXG4gKiBuZXN0aW5nLiBZb3UgbWF5IHdhbnQgdG8gdXNlXG4gKiBbRXZlbnRcbiAqIFNpZ25hdHVyZXNdKGh0dHA6Ly9nb29nbGUuZ2l0aHViLmlvL3RyYWNpbmctZnJhbWV3b3JrL2luc3RydW1lbnRpbmctY29kZS5odG1sI2N1c3RvbS1ldmVudHMpXG4gKiBhcyB0aGV5IGFyZSBkZWZpbmVkIGluIFdURi5cbiAqXG4gKiBVc2VkIHRvIG1hcmsgc2NvcGUgZW50cnkuIFRoZSByZXR1cm4gdmFsdWUgaXMgdXNlZCB0byBsZWF2ZSB0aGUgc2NvcGUuXG4gKlxuICogICAgIHZhciBteVNjb3BlID0gd3RmQ3JlYXRlU2NvcGUoJ015Q2xhc3MjbXlNZXRob2QoYXNjaWkgc29tZVZhbCknKTtcbiAqXG4gKiAgICAgc29tZU1ldGhvZCgpIHtcbiAqICAgICAgICB2YXIgcyA9IG15U2NvcGUoJ0ZvbycpOyAvLyAnRm9vJyBnZXRzIHN0b3JlZCBpbiB0cmFjaW5nIFVJXG4gKiAgICAgICAgLy8gRE8gU09NRSBXT1JLIEhFUkVcbiAqICAgICAgICByZXR1cm4gd3RmTGVhdmUocywgMTIzKTsgLy8gUmV0dXJuIHZhbHVlIDEyM1xuICogICAgIH1cbiAqXG4gKiBOb3RlLCBhZGRpbmcgdHJ5LWZpbmFsbHkgYmxvY2sgYXJvdW5kIHRoZSB3b3JrIHRvIGVuc3VyZSB0aGF0IGB3dGZMZWF2ZWAgZ2V0cyBjYWxsZWQgY2FuXG4gKiBuZWdhdGl2ZWx5IGltcGFjdCB0aGUgcGVyZm9ybWFuY2Ugb2YgeW91ciBhcHBsaWNhdGlvbi4gRm9yIHRoaXMgcmVhc29uIHdlIHJlY29tbWVuZCB0aGF0XG4gKiB5b3UgZG9uJ3QgYWRkIHRoZW0gdG8gZW5zdXJlIHRoYXQgYHd0ZkxlYXZlYCBnZXRzIGNhbGxlZC4gSW4gcHJvZHVjdGlvbiBgd3RmTGVhdmVgIGlzIGEgbm9vcCBhbmRcbiAqIHNvIHRyeS1maW5hbGx5IGJsb2NrIGhhcyBubyB2YWx1ZS4gV2hlbiBkZWJ1Z2dpbmcgcGVyZiBpc3N1ZXMsIHNraXBwaW5nIGB3dGZMZWF2ZWAsIGRvIHRvXG4gKiBleGNlcHRpb24sIHdpbGwgcHJvZHVjZSBpbmNvcnJlY3QgdHJhY2UsIGJ1dCBwcmVzZW5jZSBvZiBleGNlcHRpb24gc2lnbmlmaWVzIGxvZ2ljIGVycm9yIHdoaWNoXG4gKiBuZWVkcyB0byBiZSBmaXhlZCBiZWZvcmUgdGhlIGFwcCBzaG91bGQgYmUgcHJvZmlsZWQuIEFkZCB0cnktZmluYWxseSBvbmx5IHdoZW4geW91IGV4cGVjdCB0aGF0XG4gKiBhbiBleGNlcHRpb24gaXMgZXhwZWN0ZWQgZHVyaW5nIG5vcm1hbCBleGVjdXRpb24gd2hpbGUgcHJvZmlsaW5nLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqIEBkZXByZWNhdGVkIHRoZSBXZWIgVHJhY2luZyBGcmFtZXdvcmsgaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZCBpbiBBbmd1bGFyXG4gKi9cbmV4cG9ydCBjb25zdCB3dGZDcmVhdGVTY29wZTogKHNpZ25hdHVyZTogc3RyaW5nLCBmbGFncz86IGFueSkgPT4gV3RmU2NvcGVGbiA9XG4gICAgd3RmRW5hYmxlZCA/IGNyZWF0ZVNjb3BlIDogKHNpZ25hdHVyZTogc3RyaW5nLCBmbGFncz86IGFueSkgPT4gbm9vcFNjb3BlO1xuXG4vKipcbiAqIFVzZWQgdG8gbWFyayBlbmQgb2YgU2NvcGUuXG4gKlxuICogLSBgc2NvcGVgIHRvIGVuZC5cbiAqIC0gYHJldHVyblZhbHVlYCAob3B0aW9uYWwpIHRvIGJlIHBhc3NlZCB0byB0aGUgV1RGLlxuICpcbiAqIFJldHVybnMgdGhlIGByZXR1cm5WYWx1ZSBmb3IgZWFzeSBjaGFpbmluZy5cbiAqIEBwdWJsaWNBcGlcbiAqIEBkZXByZWNhdGVkIHRoZSBXZWIgVHJhY2luZyBGcmFtZXdvcmsgaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZCBpbiBBbmd1bGFyXG4gKi9cbmV4cG9ydCBjb25zdCB3dGZMZWF2ZTogPFQ+KHNjb3BlOiBhbnksIHJldHVyblZhbHVlPzogVCkgPT4gVCA9XG4gICAgd3RmRW5hYmxlZCA/IGxlYXZlIDogKHM6IGFueSwgcj86IGFueSkgPT4gcjtcblxuLyoqXG4gKiBVc2VkIHRvIG1hcmsgQXN5bmMgc3RhcnQuIEFzeW5jIGFyZSBzaW1pbGFyIHRvIHNjb3BlIGJ1dCB0aGV5IGRvbid0IGhhdmUgdG8gYmUgc3RyaWN0bHkgbmVzdGVkLlxuICogVGhlIHJldHVybiB2YWx1ZSBpcyB1c2VkIGluIHRoZSBjYWxsIHRvIFtlbmRBc3luY10uIEFzeW5jIHJhbmdlcyBvbmx5IHdvcmsgaWYgV1RGIGhhcyBiZWVuXG4gKiBlbmFibGVkLlxuICpcbiAqICAgICBzb21lTWV0aG9kKCkge1xuICogICAgICAgIHZhciBzID0gd3RmU3RhcnRUaW1lUmFuZ2UoJ0hUVFA6R0VUJywgJ3NvbWUudXJsJyk7XG4gKiAgICAgICAgdmFyIGZ1dHVyZSA9IG5ldyBGdXR1cmUuZGVsYXkoNSkudGhlbigoXykge1xuICogICAgICAgICAgd3RmRW5kVGltZVJhbmdlKHMpO1xuICogICAgICAgIH0pO1xuICogICAgIH1cbiAqIEBwdWJsaWNBcGlcbiAqIEBkZXByZWNhdGVkIHRoZSBXZWIgVHJhY2luZyBGcmFtZXdvcmsgaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZCBpbiBBbmd1bGFyXG4gKi9cbmV4cG9ydCBjb25zdCB3dGZTdGFydFRpbWVSYW5nZTogKHJhbmdlVHlwZTogc3RyaW5nLCBhY3Rpb246IHN0cmluZykgPT4gYW55ID1cbiAgICB3dGZFbmFibGVkID8gc3RhcnRUaW1lUmFuZ2UgOiAocmFuZ2VUeXBlOiBzdHJpbmcsIGFjdGlvbjogc3RyaW5nKSA9PiBudWxsO1xuXG4vKipcbiAqIEVuZHMgYSBhc3luYyB0aW1lIHJhbmdlIG9wZXJhdGlvbi5cbiAqIFtyYW5nZV0gaXMgdGhlIHJldHVybiB2YWx1ZSBmcm9tIFt3dGZTdGFydFRpbWVSYW5nZV0gQXN5bmMgcmFuZ2VzIG9ubHkgd29yayBpZiBXVEYgaGFzIGJlZW5cbiAqIGVuYWJsZWQuXG4gKiBAcHVibGljQXBpXG4gKiBAZGVwcmVjYXRlZCB0aGUgV2ViIFRyYWNpbmcgRnJhbWV3b3JrIGlzIG5vIGxvbmdlciBzdXBwb3J0ZWQgaW4gQW5ndWxhclxuICovXG5leHBvcnQgY29uc3Qgd3RmRW5kVGltZVJhbmdlOiAocmFuZ2U6IGFueSkgPT4gdm9pZCA9IHd0ZkVuYWJsZWQgPyBlbmRUaW1lUmFuZ2UgOiAocjogYW55KSA9PiBudWxsO1xuIl19