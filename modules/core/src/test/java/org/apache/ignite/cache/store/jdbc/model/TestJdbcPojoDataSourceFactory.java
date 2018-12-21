package org.apache.ignite.cache.store.jdbc.model;

import java.util.Objects;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;

/**
 * Test JDBC POJO DataSource factory.
 */
public class TestJdbcPojoDataSourceFactory implements Factory<DataSource> {
    /** */
    private String URL;

    /** */
    private String userName;

    /** */
    private String password;

    /** {@inheritDoc} */
    @Override public DataSource create() {
        TestJdbcPojoDataSource ds = new TestJdbcPojoDataSource();

        ds.setUrl("jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1");

        ds.setUsername("sa");

        ds.setPassword("");

        return ds;
    }

    /** */
    public String getURL() {
        return URL;
    }

    /** */
    public void setURL(String URL) {
        this.URL = URL;
    }

    /** */
    public String getUserName() {
        return userName;
    }

    /** */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /** */
    public String getPassword() {
        return password;
    }

    /** */
    public void setPassword(String password) {
        this.password = password;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestJdbcPojoDataSourceFactory factory = (TestJdbcPojoDataSourceFactory)o;
        return Objects.equals(URL, factory.URL) &&
            Objects.equals(userName, factory.userName) &&
            Objects.equals(password, factory.password);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {

        return Objects.hash(URL, userName, password);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestJdbcPojoDataSourceFactory{" +
            "URL='" + URL + '\'' +
            ", userName='" + userName + '\'' +
            ", password='" + password + '\'' +
            '}';
    }
}
