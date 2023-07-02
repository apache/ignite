export const dbPresets = [
        {
            db: 'Oracle',
            jdbcDriverClass: 'oracle.jdbc.OracleDriver',
            jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
            user: 'system',
            samples: true
        },
        {
            db: 'DB2',
            jdbcDriverClass: 'com.ibm.db2.jcc.DB2Driver',
            jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
            user: 'db2admin'
        },
        {
            db: 'SQLServer',
            jdbcDriverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]'
        },
        {
            db: 'PostgreSQL',
            jdbcDriverClass: 'org.postgresql.Driver',
            jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
            user: 'sa'
        },
        {
            db: 'MySQL',
            jdbcDriverClass: 'com.mysql.jdbc.Driver',
            jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
            user: 'root'
        },        
        {
            db: 'MySQL Mariadb',
            jdbcDriverClass: 'org.mariadb.jdbc.Driver',
            jdbcUrl: 'jdbc:mariadb://[host]:[port]/[database]',
            user: 'root'
        },
        {
            db: 'H2',
            jdbcDriverClass: 'org.h2.Driver',
            jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
            user: 'sa'
        },
        {
            db: 'Dremio',
            jdbcDriverClass: 'com.dremio.jdbc.Driver.',
            jdbcUrl: 'jdbc:dremio:direct=[host]:31010;schema=[OPTIONAL_SCHMEMA]',
            user: 'root'
        },
        {
            db: 'Hive',
            jdbcDriverClass: 'org.apache.hive.jdbc.HiveDriver',
            jdbcUrl: 'jdbc:hive2://[host]:[port]/[database]',
            user: 'hiveuser'
        }
    ];