package de.bwaldvogel.mongo;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


import de.bwaldvogel.mongo.backend.postgresql.PostgresqlBackend;
import io.netty.util.Signal;

public class PostgresqlMongoServer extends MongoServer {

	private static final Logger log = LoggerFactory.getLogger(PostgresqlMongoServer.class);

	private HikariDataSource dataSource;

	public void closeDataSource() {
		dataSource.close();
	}

	public static void main(String[] args) throws Exception {

		final MongoServer mongoServer;

		HikariConfig config = new HikariConfig();

		config.setMaximumPoolSize(8);

		if (args.length >= 1) {
			String jdbcUrl = args[0];
			config.setJdbcUrl(jdbcUrl);
			for(int i=1;i<args.length;i++) {
				if(args[i].equals("-u")) {
					config.setUsername(args[++i]);
				}
				else if(args[i].equals("-p")) {
					config.setPassword(args[++i]);
				}
			}
		} else {
			config.setJdbcUrl("jdbc:postgresql://localhost/postgres");
			config.setUsername("postgres");
			config.setPassword("332584185");
		}

		HikariDataSource dataSource = new HikariDataSource(config);

		mongoServer = new PostgresqlMongoServer(dataSource);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("shutting down {}", mongoServer);
				mongoServer.shutdownNow();
			}
		});

		mongoServer.bind("0.0.0.0", 27017);

	}

	public PostgresqlMongoServer(HikariDataSource dataSource) {
		super(new PostgresqlBackend(dataSource));
		this.dataSource = dataSource;
	}
}
