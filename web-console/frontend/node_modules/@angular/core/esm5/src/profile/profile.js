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
 */
export var wtfEnabled = detectWTF();
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
 * @publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 */
export var wtfCreateScope = wtfEnabled ? createScope : function (signature, flags) { return noopScope; };
/**
 * Used to mark end of Scope.
 *
 * - `scope` to end.
 * - `returnValue` (optional) to be passed to the WTF.
 *
 * Returns the `returnValue for easy chaining.
 * @publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 */
export var wtfLeave = wtfEnabled ? leave : function (s, r) { return r; };
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
 * @publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 */
export var wtfStartTimeRange = wtfEnabled ? startTimeRange : function (rangeType, action) { return null; };
/**
 * Ends a async time range operation.
 * [range] is the return value from [wtfStartTimeRange] Async ranges only work if WTF has been
 * enabled.
 * @publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 */
export var wtfEndTimeRange = wtfEnabled ? endTimeRange : function (r) { return null; };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvZmlsZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3Byb2ZpbGUvcHJvZmlsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSCxPQUFPLEVBQWEsV0FBVyxFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsS0FBSyxFQUFFLGNBQWMsRUFBQyxNQUFNLFlBQVksQ0FBQztBQUtuRzs7R0FFRztBQUNILE1BQU0sQ0FBQyxJQUFNLFVBQVUsR0FBRyxTQUFTLEVBQUUsQ0FBQztBQUV0QyxTQUFTLFNBQVMsQ0FBQyxJQUFVLEVBQUUsSUFBVTtJQUN2QyxPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7QUFFRDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBOEJHO0FBQ0gsTUFBTSxDQUFDLElBQU0sY0FBYyxHQUN2QixVQUFVLENBQUMsQ0FBQyxDQUFDLFdBQVcsQ0FBQyxDQUFDLENBQUMsVUFBQyxTQUFpQixFQUFFLEtBQVcsSUFBSyxPQUFBLFNBQVMsRUFBVCxDQUFTLENBQUM7QUFFN0U7Ozs7Ozs7OztHQVNHO0FBQ0gsTUFBTSxDQUFDLElBQU0sUUFBUSxHQUNqQixVQUFVLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsVUFBQyxDQUFNLEVBQUUsQ0FBTyxJQUFLLE9BQUEsQ0FBQyxFQUFELENBQUMsQ0FBQztBQUVoRDs7Ozs7Ozs7Ozs7OztHQWFHO0FBQ0gsTUFBTSxDQUFDLElBQU0saUJBQWlCLEdBQzFCLFVBQVUsQ0FBQyxDQUFDLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQyxVQUFDLFNBQWlCLEVBQUUsTUFBYyxJQUFLLE9BQUEsSUFBSSxFQUFKLENBQUksQ0FBQztBQUU5RTs7Ozs7O0dBTUc7QUFDSCxNQUFNLENBQUMsSUFBTSxlQUFlLEdBQXlCLFVBQVUsQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxVQUFDLENBQU0sSUFBSyxPQUFBLElBQUksRUFBSixDQUFJLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7V3RmU2NvcGVGbiwgY3JlYXRlU2NvcGUsIGRldGVjdFdURiwgZW5kVGltZVJhbmdlLCBsZWF2ZSwgc3RhcnRUaW1lUmFuZ2V9IGZyb20gJy4vd3RmX2ltcGwnO1xuXG5leHBvcnQge1d0ZlNjb3BlRm59IGZyb20gJy4vd3RmX2ltcGwnO1xuXG5cbi8qKlxuICogVHJ1ZSBpZiBXVEYgaXMgZW5hYmxlZC5cbiAqL1xuZXhwb3J0IGNvbnN0IHd0ZkVuYWJsZWQgPSBkZXRlY3RXVEYoKTtcblxuZnVuY3Rpb24gbm9vcFNjb3BlKGFyZzA/OiBhbnksIGFyZzE/OiBhbnkpOiBhbnkge1xuICByZXR1cm4gbnVsbDtcbn1cblxuLyoqXG4gKiBDcmVhdGUgdHJhY2Ugc2NvcGUuXG4gKlxuICogU2NvcGVzIG11c3QgYmUgc3RyaWN0bHkgbmVzdGVkIGFuZCBhcmUgYW5hbG9nb3VzIHRvIHN0YWNrIGZyYW1lcywgYnV0XG4gKiBkbyBub3QgaGF2ZSB0byBmb2xsb3cgdGhlIHN0YWNrIGZyYW1lcy4gSW5zdGVhZCBpdCBpcyByZWNvbW1lbmRlZCB0aGF0IHRoZXkgZm9sbG93IGxvZ2ljYWxcbiAqIG5lc3RpbmcuIFlvdSBtYXkgd2FudCB0byB1c2VcbiAqIFtFdmVudFxuICogU2lnbmF0dXJlc10oaHR0cDovL2dvb2dsZS5naXRodWIuaW8vdHJhY2luZy1mcmFtZXdvcmsvaW5zdHJ1bWVudGluZy1jb2RlLmh0bWwjY3VzdG9tLWV2ZW50cylcbiAqIGFzIHRoZXkgYXJlIGRlZmluZWQgaW4gV1RGLlxuICpcbiAqIFVzZWQgdG8gbWFyayBzY29wZSBlbnRyeS4gVGhlIHJldHVybiB2YWx1ZSBpcyB1c2VkIHRvIGxlYXZlIHRoZSBzY29wZS5cbiAqXG4gKiAgICAgdmFyIG15U2NvcGUgPSB3dGZDcmVhdGVTY29wZSgnTXlDbGFzcyNteU1ldGhvZChhc2NpaSBzb21lVmFsKScpO1xuICpcbiAqICAgICBzb21lTWV0aG9kKCkge1xuICogICAgICAgIHZhciBzID0gbXlTY29wZSgnRm9vJyk7IC8vICdGb28nIGdldHMgc3RvcmVkIGluIHRyYWNpbmcgVUlcbiAqICAgICAgICAvLyBETyBTT01FIFdPUksgSEVSRVxuICogICAgICAgIHJldHVybiB3dGZMZWF2ZShzLCAxMjMpOyAvLyBSZXR1cm4gdmFsdWUgMTIzXG4gKiAgICAgfVxuICpcbiAqIE5vdGUsIGFkZGluZyB0cnktZmluYWxseSBibG9jayBhcm91bmQgdGhlIHdvcmsgdG8gZW5zdXJlIHRoYXQgYHd0ZkxlYXZlYCBnZXRzIGNhbGxlZCBjYW5cbiAqIG5lZ2F0aXZlbHkgaW1wYWN0IHRoZSBwZXJmb3JtYW5jZSBvZiB5b3VyIGFwcGxpY2F0aW9uLiBGb3IgdGhpcyByZWFzb24gd2UgcmVjb21tZW5kIHRoYXRcbiAqIHlvdSBkb24ndCBhZGQgdGhlbSB0byBlbnN1cmUgdGhhdCBgd3RmTGVhdmVgIGdldHMgY2FsbGVkLiBJbiBwcm9kdWN0aW9uIGB3dGZMZWF2ZWAgaXMgYSBub29wIGFuZFxuICogc28gdHJ5LWZpbmFsbHkgYmxvY2sgaGFzIG5vIHZhbHVlLiBXaGVuIGRlYnVnZ2luZyBwZXJmIGlzc3Vlcywgc2tpcHBpbmcgYHd0ZkxlYXZlYCwgZG8gdG9cbiAqIGV4Y2VwdGlvbiwgd2lsbCBwcm9kdWNlIGluY29ycmVjdCB0cmFjZSwgYnV0IHByZXNlbmNlIG9mIGV4Y2VwdGlvbiBzaWduaWZpZXMgbG9naWMgZXJyb3Igd2hpY2hcbiAqIG5lZWRzIHRvIGJlIGZpeGVkIGJlZm9yZSB0aGUgYXBwIHNob3VsZCBiZSBwcm9maWxlZC4gQWRkIHRyeS1maW5hbGx5IG9ubHkgd2hlbiB5b3UgZXhwZWN0IHRoYXRcbiAqIGFuIGV4Y2VwdGlvbiBpcyBleHBlY3RlZCBkdXJpbmcgbm9ybWFsIGV4ZWN1dGlvbiB3aGlsZSBwcm9maWxpbmcuXG4gKlxuICogQHB1YmxpY0FwaVxuICogQGRlcHJlY2F0ZWQgdGhlIFdlYiBUcmFjaW5nIEZyYW1ld29yayBpcyBubyBsb25nZXIgc3VwcG9ydGVkIGluIEFuZ3VsYXJcbiAqL1xuZXhwb3J0IGNvbnN0IHd0ZkNyZWF0ZVNjb3BlOiAoc2lnbmF0dXJlOiBzdHJpbmcsIGZsYWdzPzogYW55KSA9PiBXdGZTY29wZUZuID1cbiAgICB3dGZFbmFibGVkID8gY3JlYXRlU2NvcGUgOiAoc2lnbmF0dXJlOiBzdHJpbmcsIGZsYWdzPzogYW55KSA9PiBub29wU2NvcGU7XG5cbi8qKlxuICogVXNlZCB0byBtYXJrIGVuZCBvZiBTY29wZS5cbiAqXG4gKiAtIGBzY29wZWAgdG8gZW5kLlxuICogLSBgcmV0dXJuVmFsdWVgIChvcHRpb25hbCkgdG8gYmUgcGFzc2VkIHRvIHRoZSBXVEYuXG4gKlxuICogUmV0dXJucyB0aGUgYHJldHVyblZhbHVlIGZvciBlYXN5IGNoYWluaW5nLlxuICogQHB1YmxpY0FwaVxuICogQGRlcHJlY2F0ZWQgdGhlIFdlYiBUcmFjaW5nIEZyYW1ld29yayBpcyBubyBsb25nZXIgc3VwcG9ydGVkIGluIEFuZ3VsYXJcbiAqL1xuZXhwb3J0IGNvbnN0IHd0ZkxlYXZlOiA8VD4oc2NvcGU6IGFueSwgcmV0dXJuVmFsdWU/OiBUKSA9PiBUID1cbiAgICB3dGZFbmFibGVkID8gbGVhdmUgOiAoczogYW55LCByPzogYW55KSA9PiByO1xuXG4vKipcbiAqIFVzZWQgdG8gbWFyayBBc3luYyBzdGFydC4gQXN5bmMgYXJlIHNpbWlsYXIgdG8gc2NvcGUgYnV0IHRoZXkgZG9uJ3QgaGF2ZSB0byBiZSBzdHJpY3RseSBuZXN0ZWQuXG4gKiBUaGUgcmV0dXJuIHZhbHVlIGlzIHVzZWQgaW4gdGhlIGNhbGwgdG8gW2VuZEFzeW5jXS4gQXN5bmMgcmFuZ2VzIG9ubHkgd29yayBpZiBXVEYgaGFzIGJlZW5cbiAqIGVuYWJsZWQuXG4gKlxuICogICAgIHNvbWVNZXRob2QoKSB7XG4gKiAgICAgICAgdmFyIHMgPSB3dGZTdGFydFRpbWVSYW5nZSgnSFRUUDpHRVQnLCAnc29tZS51cmwnKTtcbiAqICAgICAgICB2YXIgZnV0dXJlID0gbmV3IEZ1dHVyZS5kZWxheSg1KS50aGVuKChfKSB7XG4gKiAgICAgICAgICB3dGZFbmRUaW1lUmFuZ2Uocyk7XG4gKiAgICAgICAgfSk7XG4gKiAgICAgfVxuICogQHB1YmxpY0FwaVxuICogQGRlcHJlY2F0ZWQgdGhlIFdlYiBUcmFjaW5nIEZyYW1ld29yayBpcyBubyBsb25nZXIgc3VwcG9ydGVkIGluIEFuZ3VsYXJcbiAqL1xuZXhwb3J0IGNvbnN0IHd0ZlN0YXJ0VGltZVJhbmdlOiAocmFuZ2VUeXBlOiBzdHJpbmcsIGFjdGlvbjogc3RyaW5nKSA9PiBhbnkgPVxuICAgIHd0ZkVuYWJsZWQgPyBzdGFydFRpbWVSYW5nZSA6IChyYW5nZVR5cGU6IHN0cmluZywgYWN0aW9uOiBzdHJpbmcpID0+IG51bGw7XG5cbi8qKlxuICogRW5kcyBhIGFzeW5jIHRpbWUgcmFuZ2Ugb3BlcmF0aW9uLlxuICogW3JhbmdlXSBpcyB0aGUgcmV0dXJuIHZhbHVlIGZyb20gW3d0ZlN0YXJ0VGltZVJhbmdlXSBBc3luYyByYW5nZXMgb25seSB3b3JrIGlmIFdURiBoYXMgYmVlblxuICogZW5hYmxlZC5cbiAqIEBwdWJsaWNBcGlcbiAqIEBkZXByZWNhdGVkIHRoZSBXZWIgVHJhY2luZyBGcmFtZXdvcmsgaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZCBpbiBBbmd1bGFyXG4gKi9cbmV4cG9ydCBjb25zdCB3dGZFbmRUaW1lUmFuZ2U6IChyYW5nZTogYW55KSA9PiB2b2lkID0gd3RmRW5hYmxlZCA/IGVuZFRpbWVSYW5nZSA6IChyOiBhbnkpID0+IG51bGw7XG4iXX0=