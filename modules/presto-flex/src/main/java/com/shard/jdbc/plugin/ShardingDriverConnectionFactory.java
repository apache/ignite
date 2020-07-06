/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shard.jdbc.plugin;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.shard.jdbc.util.DbUtil;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;

public class ShardingDriverConnectionFactory extends DriverConnectionFactory
        implements ConnectionFactory
{
	ShardingJdbcConfig metaDataConfig;
	


	public static Driver setupDriver(String driverClassName) {
		
		try {
			//Driver driver = org.postgresql.Driver();
			Class<Driver> cls = (Class<Driver>) Class.forName(driverClassName);
			Constructor<Driver> con = cls.getConstructor();
			con.setAccessible(true);
			return con.newInstance();
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(String.valueOf(e.getMessage()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IllegalStateException(String.valueOf(e.getMessage()));
		}

	}

    public ShardingDriverConnectionFactory(ShardingJdbcConfig metaDataConfig, BaseJdbcConfig config,Properties connectionProperties,CredentialProvider credentialProvider)
    {    	
        super(setupDriver(metaDataConfig.getDriver()), config.getConnectionUrl(), connectionProperties,credentialProvider);
        this.metaDataConfig = metaDataConfig;
        DbUtil.init(metaDataConfig.getShardingRulePath());
    }

   


    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection metaConnection = super.openConnection(identity);
        return metaConnection;
    }
}
