export const dbPresets = [
        {
            db: 'Oracle',
            driverCls: 'oracle.jdbc.OracleDriver',
            jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
            user: 'system',
            samples: true
        },
        {
            db: 'DB2',
            driverCls: 'com.ibm.db2.jcc.DB2Driver',
            jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
            user: 'db2admin'
        },
        {
            db: 'SQLServer',
            driverCls: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]'
        },
        {
            db: 'PostgreSQL',
            driverCls: 'org.postgresql.Driver',
            jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
            user: 'sa'
        },
        {
            db: 'MySQL',
            driverCls: 'com.mysql.jdbc.Driver',
            jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
            user: 'root'
        },        
        {
            db: 'MySQL Mariadb',
            driverCls: 'org.mariadb.jdbc.Driver',
            jdbcUrl: 'jdbc:mariadb://[host]:[port]/[database]',
            user: 'root'
        },
        {
            db: 'H2',
            driverCls: 'org.h2.Driver',
            jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
            user: 'sa'
        },
        {
            db: 'Dremio',
            driverCls: 'com.dremio.jdbc.Driver.',
            jdbcUrl: 'jdbc:dremio:direct=[host]:31010;schema=[OPTIONAL_SCHMEMA]',
            user: 'root'
        },
        {
            db: 'Hive',
            driverCls: 'org.apache.hive.jdbc.HiveDriver',
            jdbcUrl: 'jdbc:hive2://[host]:[port]/[database]',
            user: 'hiveuser'
        }
    ];