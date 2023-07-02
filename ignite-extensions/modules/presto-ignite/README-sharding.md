## ShardingJdbc
横向扩展jdbc



##Another jdbc shard framework##

This is an jdbc shard framework, add shard support to your jdbc code。
It mainly has two configuration files——database.xml and shard.xml

###database.xml——configure database and its connection info.###

    <databases>
        <database id="data1">
            <url>jdbc:mysql://localhost:3306/data1</url>
            <driver>com.mysql.jdbc.Driver</driver>
            <username>root</username>
            <password>root</password>
        </database>
    </databases>

Just simply configure all your database configuration here.
Every database connection is a database node.

###shard.xml——configure shard configuration.###

+ Just feel easy to add the following code to indicate Student to be shard by hash.

        <shard class="com.shun.domain.Student" shard-field="id" type="hash">
            <match match="0">data1</match>
            <match match="1">data2</match>
            <match match="2">data3</match>
            <match match="3">data4</match>
        </shard>

+ range is like this:

        <match match="0~20">data1</match>
        <match match="21~40">data2</match>
        <match match="41~60">data3</match>
        <match match="61~80">data4</match>

+ range-hash like this:

        <shard class="Teacher" type="range-hash">
            <range range="0~30">
                <match match="0">data1</match>
                <match match="1">data2</match>
                <match match="2">data3</match>
                <match match="3">data4</match>
            </range>
        </shard>

+ no need to shard:

        <shard class="Course" type="none">
            <match>data4</match>
        </shard>

Just simply configure all your domain here, if it need to be shard, just configure type to has(currently support hash, range, range-hash, none)
Every class map to an shard node, every match match to the value after hash.

> when you attempt to use it, just simply import use the following code:
```Connection conn = DbUtil.getConnection(clazz, new Shard(clazz, shardVal));```
Then everything is just like what you've done in jdbc programming.

+ spring support

		DataSource dataSource = DbUtil.getDataSource(Teacher.class, new Shard(Teacher.class, 1));
	
	    ShardJdbcTemplate shardJdbcTemplate = new ShardJdbcTemplate(dataSource);

Enjoy it.

What is on plan:

+ ORM support
+ spring jdbc annotation support
+ code completion to make it more easy to maintain