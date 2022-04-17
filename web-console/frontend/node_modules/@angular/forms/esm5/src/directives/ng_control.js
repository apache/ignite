/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { AbstractControlDirective } from './abstract_control_directive';
function unimplemented() {
    throw new Error('unimplemented');
}
/**
 * @description
 * A base class that all control `FormControl`-based directives extend. It binds a `FormControl`
 * object to a DOM element.
 *
 * @publicApi
 */
var NgControl = /** @class */ (function (_super) {
    tslib_1.__extends(NgControl, _super);
    function NgControl() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        /**
         * @description
         * The parent form for the control.
         *
         * @internal
         */
        _this._parent = null;
        /**
         * @description
         * The name for the control
         */
        _this.name = null;
        /**
         * @description
         * The value accessor for the control
         */
        _this.valueAccessor = null;
        /**
         * @description
         * The uncomposed array of synchronous validators for the control
         *
         * @internal
         */
        _this._rawValidators = [];
        /**
         * @description
         * The uncomposed array of async validators for the control
         *
         * @internal
         */
        _this._rawAsyncValidators = [];
        return _this;
    }
    Object.defineProperty(NgControl.prototype, "validator", {
        /**
         * @description
         * The registered synchronous validator function for the control
         *
         * @throws An exception that this method is not implemented
         */
        get: function () { return unimplemented(); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgControl.prototype, "asyncValidator", {
        /**
         * @description
         * The registered async validator function for the control
         *
         * @throws An exception that this method is not implemented
         */
        get: function () { return unimplemented(); },
        enumerable: true,
        configurable: true
    });
    return NgControl;
}(AbstractControlDirective));
export { NgControl };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29udHJvbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL25nX2NvbnRyb2wudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUdILE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLDhCQUE4QixDQUFDO0FBS3RFLFNBQVMsYUFBYTtJQUNwQixNQUFNLElBQUksS0FBSyxDQUFDLGVBQWUsQ0FBQyxDQUFDO0FBQ25DLENBQUM7QUFFRDs7Ozs7O0dBTUc7QUFDSDtJQUF3QyxxQ0FBd0I7SUFBaEU7UUFBQSxxRUE0REM7UUEzREM7Ozs7O1dBS0c7UUFDSCxhQUFPLEdBQTBCLElBQUksQ0FBQztRQUV0Qzs7O1dBR0c7UUFDSCxVQUFJLEdBQWdCLElBQUksQ0FBQztRQUV6Qjs7O1dBR0c7UUFDSCxtQkFBYSxHQUE4QixJQUFJLENBQUM7UUFFaEQ7Ozs7O1dBS0c7UUFDSCxvQkFBYyxHQUFpQyxFQUFFLENBQUM7UUFFbEQ7Ozs7O1dBS0c7UUFDSCx5QkFBbUIsR0FBMkMsRUFBRSxDQUFDOztJQXlCbkUsQ0FBQztJQWpCQyxzQkFBSSxnQ0FBUztRQU5iOzs7OztXQUtHO2FBQ0gsY0FBb0MsT0FBb0IsYUFBYSxFQUFFLENBQUMsQ0FBQyxDQUFDOzs7T0FBQTtJQVExRSxzQkFBSSxxQ0FBYztRQU5sQjs7Ozs7V0FLRzthQUNILGNBQThDLE9BQXlCLGFBQWEsRUFBRSxDQUFDLENBQUMsQ0FBQzs7O09BQUE7SUFTM0YsZ0JBQUM7QUFBRCxDQUFDLEFBNURELENBQXdDLHdCQUF3QixHQTREL0QiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cblxuaW1wb3J0IHtBYnN0cmFjdENvbnRyb2xEaXJlY3RpdmV9IGZyb20gJy4vYWJzdHJhY3RfY29udHJvbF9kaXJlY3RpdmUnO1xuaW1wb3J0IHtDb250cm9sQ29udGFpbmVyfSBmcm9tICcuL2NvbnRyb2xfY29udGFpbmVyJztcbmltcG9ydCB7Q29udHJvbFZhbHVlQWNjZXNzb3J9IGZyb20gJy4vY29udHJvbF92YWx1ZV9hY2Nlc3Nvcic7XG5pbXBvcnQge0FzeW5jVmFsaWRhdG9yLCBBc3luY1ZhbGlkYXRvckZuLCBWYWxpZGF0b3IsIFZhbGlkYXRvckZufSBmcm9tICcuL3ZhbGlkYXRvcnMnO1xuXG5mdW5jdGlvbiB1bmltcGxlbWVudGVkKCk6IGFueSB7XG4gIHRocm93IG5ldyBFcnJvcigndW5pbXBsZW1lbnRlZCcpO1xufVxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICogQSBiYXNlIGNsYXNzIHRoYXQgYWxsIGNvbnRyb2wgYEZvcm1Db250cm9sYC1iYXNlZCBkaXJlY3RpdmVzIGV4dGVuZC4gSXQgYmluZHMgYSBgRm9ybUNvbnRyb2xgXG4gKiBvYmplY3QgdG8gYSBET00gZWxlbWVudC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBOZ0NvbnRyb2wgZXh0ZW5kcyBBYnN0cmFjdENvbnRyb2xEaXJlY3RpdmUge1xuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSBwYXJlbnQgZm9ybSBmb3IgdGhlIGNvbnRyb2wuXG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgX3BhcmVudDogQ29udHJvbENvbnRhaW5lcnxudWxsID0gbnVsbDtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSBuYW1lIGZvciB0aGUgY29udHJvbFxuICAgKi9cbiAgbmFtZTogc3RyaW5nfG51bGwgPSBudWxsO1xuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIHZhbHVlIGFjY2Vzc29yIGZvciB0aGUgY29udHJvbFxuICAgKi9cbiAgdmFsdWVBY2Nlc3NvcjogQ29udHJvbFZhbHVlQWNjZXNzb3J8bnVsbCA9IG51bGw7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgdW5jb21wb3NlZCBhcnJheSBvZiBzeW5jaHJvbm91cyB2YWxpZGF0b3JzIGZvciB0aGUgY29udHJvbFxuICAgKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIF9yYXdWYWxpZGF0b3JzOiBBcnJheTxWYWxpZGF0b3J8VmFsaWRhdG9yRm4+ID0gW107XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgdW5jb21wb3NlZCBhcnJheSBvZiBhc3luYyB2YWxpZGF0b3JzIGZvciB0aGUgY29udHJvbFxuICAgKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIF9yYXdBc3luY1ZhbGlkYXRvcnM6IEFycmF5PEFzeW5jVmFsaWRhdG9yfEFzeW5jVmFsaWRhdG9yRm4+ID0gW107XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgcmVnaXN0ZXJlZCBzeW5jaHJvbm91cyB2YWxpZGF0b3IgZnVuY3Rpb24gZm9yIHRoZSBjb250cm9sXG4gICAqXG4gICAqIEB0aHJvd3MgQW4gZXhjZXB0aW9uIHRoYXQgdGhpcyBtZXRob2QgaXMgbm90IGltcGxlbWVudGVkXG4gICAqL1xuICBnZXQgdmFsaWRhdG9yKCk6IFZhbGlkYXRvckZufG51bGwgeyByZXR1cm4gPFZhbGlkYXRvckZuPnVuaW1wbGVtZW50ZWQoKTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIHJlZ2lzdGVyZWQgYXN5bmMgdmFsaWRhdG9yIGZ1bmN0aW9uIGZvciB0aGUgY29udHJvbFxuICAgKlxuICAgKiBAdGhyb3dzIEFuIGV4Y2VwdGlvbiB0aGF0IHRoaXMgbWV0aG9kIGlzIG5vdCBpbXBsZW1lbnRlZFxuICAgKi9cbiAgZ2V0IGFzeW5jVmFsaWRhdG9yKCk6IEFzeW5jVmFsaWRhdG9yRm58bnVsbCB7IHJldHVybiA8QXN5bmNWYWxpZGF0b3JGbj51bmltcGxlbWVudGVkKCk7IH1cblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSBjYWxsYmFjayBtZXRob2QgdG8gdXBkYXRlIHRoZSBtb2RlbCBmcm9tIHRoZSB2aWV3IHdoZW4gcmVxdWVzdGVkXG4gICAqXG4gICAqIEBwYXJhbSBuZXdWYWx1ZSBUaGUgbmV3IHZhbHVlIGZvciB0aGUgdmlld1xuICAgKi9cbiAgYWJzdHJhY3Qgdmlld1RvTW9kZWxVcGRhdGUobmV3VmFsdWU6IGFueSk6IHZvaWQ7XG59XG4iXX0=