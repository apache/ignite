

import {OperatorFunction} from 'rxjs';
import {filter} from 'rxjs/operators';

type Action = {type: string};
export function ofType<T extends string, U extends Action, V extends Extract<U, {type: T}>>(type: T): OperatorFunction<U, V>
export function ofType<U extends Action>(type): OperatorFunction<U, U> {
    return filter((action: U): boolean => type === action.type);
}

