

import _ from 'lodash';

// List of H2 reserved SQL keywords.
import H2_SQL_KEYWORDS from 'app/data/sql-keywords.json';

// List of JDBC type descriptors.
import JDBC_TYPES from 'app/data/jdbc-types.json';

// Regular expression to check H2 SQL identifier.
const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_$]*$/im;

// Descriptor for unknown JDBC type.
const UNKNOWN_JDBC_TYPE = {
    dbName: 'Unknown',
    signed: {javaType: 'Unknown', primitiveType: 'Unknown'},
    unsigned: {javaType: 'Unknown', primitiveType: 'Unknown'}
};

const JDBC_TYPE_ALIASES = {
    "DATETIME": "DATE",
    "UUID":     "VARCHAR",
    "JSON":     "LONGVARCHAR"    
};

/**
 * Utility service for various check on SQL types.
 */
export default class SqlTypes {
    /**
     * @param {String} value Value to check.
     * @returns {boolean} 'true' if given text is valid sql class name.
     */
    validIdentifier(value) {
        return !!(value && VALID_IDENTIFIER.test(value));
    }

    /**
     * @param value {String} Value to check.
     * @returns {boolean} 'true' if given text is one of H2 reserved keywords.
     */
    isKeyword(value) {
        return !!(value && _.includes(H2_SQL_KEYWORDS, value.toUpperCase()));
    }

    isValidSqlIdentifier(s) {
        return this.validIdentifier(s) && !this.isKeyword(s) && !this.findJdbcTypeName(s);
    }

    /**
     * Find JDBC type descriptor for specified JDBC type and options.
     *
     * @param {Number} dbType  Column db type.
     * @return {String|object} Java type.
     */
    findJdbcType(dbType,dbTypeName) {
        let jdbcType = undefined;
        if(dbType==1111 || dbType==0 || dbType==undefined){
            jdbcType = _.find(JDBC_TYPES, (item) => item.dbName === dbTypeName);
        }
        if(!jdbcType){
            jdbcType = _.find(JDBC_TYPES, (item) => item.dbType === dbType);
        }
        return jdbcType ? jdbcType : UNKNOWN_JDBC_TYPE;
    }

    findJdbcTypeName(typeName) {
        typeName = typeName.toUpperCase();
        const jdbcType = JDBC_TYPE_ALIASES[typeName];
        return jdbcType ? jdbcType : typeName;
    }

    toJdbcIdentifier(name) { 
        const len = name.length;
        let ident = name.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase();
        return ident;
    }

    toJdbcTypeName(name) {
        const clazzName = this.toJdbcIdentifier(name);

        if (this.isValidSqlIdentifier(clazzName))
            return clazzName;

        return 'Type' + clazzName;
    }

    mkJdbcTypeOptions() {
        return _.map(JDBC_TYPES, (option) => {
            return {value: option.dbName, label: option.dbName};
        });
    }
}
