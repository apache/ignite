package com.shard.jdbc.test;

import java.sql.Connection;
import java.sql.SQLException;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

import com.shard.jdbc.domain.Building;
import com.shard.jdbc.domain.Course;
import com.shard.jdbc.domain.Student;
import com.shard.jdbc.domain.Teacher;
import com.shard.jdbc.exception.DbException;
import com.shard.jdbc.shard.Shard;
import com.shard.jdbc.util.DbUtil;

/**
 * Created by shun on 2015-12-23 17:01.
 */
public class TestDbUtil {

    @Test
    public void test() throws DbException, SQLException {
        //hash
        Connection conn = DbUtil.getConnection(Teacher.class.getName(), new Shard(Teacher.class.getName(), "id", 1));
        assertEquals(conn.getMetaData().getURL(), "jdbc:mysql://localhost:3306/data2?useSSL=false");

        //range-hash
        conn = DbUtil.getConnection(Student.class.getName(), new Shard(Student.class.getName(), "id", 4));
        assertEquals(conn.getMetaData().getURL(), "jdbc:mysql://localhost:3306/data1?useSSL=false");

        //range
        conn = DbUtil.getConnection(Building.class.getName(), new Shard(Building.class.getName(), "id", 70));
        assertEquals(conn.getMetaData().getURL(), "jdbc:mysql://localhost:3306/data4?useSSL=false");

        conn = DbUtil.getConnectionForType(Course.class.getName());
        assertEquals(conn.getMetaData().getURL(), "jdbc:mysql://localhost:3306/data4?useSSL=false");

        conn = DbUtil.getConnection(Course.class.getName(), null);
        assertEquals(conn.getMetaData().getURL(), "jdbc:mysql://localhost:3306/data4?useSSL=false");
    }

}
