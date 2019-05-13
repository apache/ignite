/** Helper class for the library methods arguments check. */
export declare class ArgumentChecker {
    static notEmpty(arg: any, argName: any): void;
    static notNull(arg: any, argName: any): void;
    static hasType(arg: any, argName: any, isArray: any, ...types: any[]): void;
    static hasValueFrom(arg: any, argName: any, isArray: any, values: any): void;
    static isInteger(arg: any, argName: any): void;
    static invalidArgument(arg: any, argName: any, type: any): void;
}
