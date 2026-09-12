
// Descriptors for generation of demo data.
export const PREDEFINED_QUERIES = [
    {
        schema: 'CARS',
        type: 'PARKING',
        create: [
            'CREATE TABLE IF NOT EXISTS CARS.PARKING (',
            'ID       INTEGER     NOT NULL PRIMARY KEY,',
            'NAME     VARCHAR(50) NOT NULL,',
            'CAPACITY INTEGER NOT NULL)'
        ],
        clearQuery: ['DELETE FROM CARS.PARKING'],
        insertCntConsts: [{name: 'DEMO_MAX_PARKING_CNT', val: 5, comment: 'How many parkings to generate.'}],
        insertPattern: ['INSERT INTO CARS.PARKING(ID, NAME, CAPACITY) VALUES(?, ?, ?)'],
        fillInsertParameters(sb) {
            sb.append('stmt.setInt(1, id);');
            sb.append('stmt.setString(2, "Parking #" + (id + 1));');
            sb.append('stmt.setInt(3, 10 + rnd.nextInt(20));');
        },
        selectQuery: ['SELECT * FROM PARKING WHERE CAPACITY >= 20']
    },
    {
        schema: 'CARS',
        type: 'CAR',
        create: [
            'CREATE TABLE IF NOT EXISTS CARS.CAR (',
            'ID         INTEGER NOT NULL PRIMARY KEY,',
            'PARKING_ID INTEGER NOT NULL,',
            'NAME       VARCHAR(50) NOT NULL);'
        ],
        clearQuery: ['DELETE FROM CARS.CAR'],
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_CAR_CNT', val: 10, comment: 'How many cars to generate.'},
            {name: 'DEMO_MAX_PARKING_CNT', val: 5, comment: 'How many parkings to generate.'}
        ],
        insertPattern: ['INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(?, ?, ?)'],
        fillInsertParameters(sb) {
            sb.append('stmt.setInt(1, id);');
            sb.append('stmt.setInt(2, rnd.nextInt(DEMO_MAX_PARKING_CNT));');
            sb.append('stmt.setString(3, "Car #" + (id + 1));');
        },
        selectQuery: ['SELECT * FROM CAR WHERE PARKINGID = 2']
    },
    {
        type: 'COUNTRY',
        create: [
            'CREATE TABLE IF NOT EXISTS COUNTRY (',
            'ID         INTEGER NOT NULL PRIMARY KEY,',
            'NAME       VARCHAR(50),',
            'POPULATION INTEGER NOT NULL);'
        ],
        clearQuery: ['DELETE FROM COUNTRY'],
        insertCntConsts: [{name: 'DEMO_MAX_COUNTRY_CNT', val: 5, comment: 'How many countries to generate.'}],
        insertPattern: ['INSERT INTO COUNTRY(ID, NAME, POPULATION) VALUES(?, ?, ?)'],
        fillInsertParameters(sb) {
            sb.append('stmt.setInt(1, id);');
            sb.append('stmt.setString(2, "Country #" + (id + 1));');
            sb.append('stmt.setInt(3, 10000000 + rnd.nextInt(100000000));');
        },
        selectQuery: ['SELECT * FROM COUNTRY WHERE POPULATION BETWEEN 15000000 AND 25000000']
    },
    {
        type: 'DEPARTMENT',
        create: [
            'CREATE TABLE IF NOT EXISTS DEPARTMENT (',
            'ID         INTEGER NOT NULL PRIMARY KEY,',
            'COUNTRY_ID INTEGER NOT NULL,',
            'NAME       VARCHAR(50) NOT NULL);'
        ],
        clearQuery: ['DELETE FROM DEPARTMENT'],
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_DEPARTMENT_CNT', val: 5, comment: 'How many departments to generate.'},
            {name: 'DEMO_MAX_COUNTRY_CNT', val: 5, comment: 'How many countries to generate.'}
        ],
        insertPattern: ['INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(?, ?, ?)'],
        fillInsertParameters(sb) {
            sb.append('stmt.setInt(1, id);');
            sb.append('stmt.setInt(2, rnd.nextInt(DEMO_MAX_COUNTRY_CNT));');
            sb.append('stmt.setString(3, "Department #" + (id + 1));');
        },
        selectQuery: ['SELECT * FROM DEPARTMENT']
    },
    {
        type: 'EMPLOYEE',
        create: [
            'CREATE TABLE IF NOT EXISTS EMPLOYEE (',
            'ID            INTEGER NOT NULL PRIMARY KEY,',
            'DEPARTMENT_ID INTEGER NOT NULL,',
            'MANAGER_ID    INTEGER,',
            'FIRST_NAME    VARCHAR(50) NOT NULL,',
            'LAST_NAME     VARCHAR(50) NOT NULL,',
            'EMAIL         VARCHAR(50) NOT NULL,',
            'PHONE_NUMBER  VARCHAR(50),',
            'HIRE_DATE     DATE        NOT NULL,',
            'JOB           VARCHAR(50) NOT NULL,',
            'SALARY        DOUBLE);'
        ],
        clearQuery: ['DELETE FROM EMPLOYEE'],
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_EMPLOYEE_CNT', val: 10, comment: 'How many employees to generate.'},
            {name: 'DEMO_MAX_DEPARTMENT_CNT', val: 5, comment: 'How many departments to generate.'}
        ],
        customGeneration(sb, conVar, stmtVar) {
            sb.append(`${stmtVar} = ${conVar}.prepareStatement("INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");`);

            sb.emptyLine();

            sb.startBlock('for (int id = 0; id < DEMO_MAX_EMPLOYEE_CNT; id ++) {');

            sb.append('int depId = rnd.nextInt(DEMO_MAX_DEPARTMENT_CNT);');

            sb.emptyLine();

            sb.append('stmt.setInt(1, DEMO_MAX_DEPARTMENT_CNT + id);');
            sb.append('stmt.setInt(2, depId);');
            sb.append('stmt.setInt(3, depId);');
            sb.append('stmt.setString(4, "First name manager #" + (id + 1));');
            sb.append('stmt.setString(5, "Last name manager#" + (id + 1));');
            sb.append('stmt.setString(6, "Email manager#" + (id + 1));');
            sb.append('stmt.setString(7, "Phone number manager#" + (id + 1));');
            sb.append('stmt.setString(8, "2014-01-01");');
            sb.append('stmt.setString(9, "Job manager #" + (id + 1));');
            sb.append('stmt.setDouble(10, 600.0 + rnd.nextInt(300));');

            sb.emptyLine();

            sb.append('stmt.executeUpdate();');

            sb.endBlock('}');
        },
        selectQuery: ['SELECT * FROM EMPLOYEE WHERE SALARY > 700']
    }
];
