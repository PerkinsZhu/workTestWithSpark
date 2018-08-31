package hive;

/**
 * Created by PerkinsZhu on 2018/8/31 17:13
 **/

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcTest {
    //beeline -u jdbc:hive2://servera:10000/ -n jinzhao -p jinzhao

    String driverName = "org.apache.hive.jdbc.HiveDriver";
    Statement stmt = null;

    @Before
    public void createStatement() {
        try {
            Class.forName(driverName);
            //注意这里的账号和密码，不是数据库mysql的账号和密码。而是hive所有linux服务器的账号和密码
            Connection con = DriverManager.getConnection("jdbc:hive2://servera.local.com:10000/sbux", "jinzhao", "jinzhao");
            stmt = con.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testShowDBs() {
        try {
            ResultSet result = stmt.executeQuery("select * from student");
            showResult(result);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void showResult(ResultSet resultSet) throws SQLException {
        int row = resultSet.getRow();
        System.out.println(row);
        while (row > 0) {
            System.out.println(resultSet.getString(1));
            row--;
        }
    }

    public void createTable(String tableName) {
        try {
            stmt.execute("drop table if exists " + tableName);
            boolean status = stmt.execute("create table " + tableName + " (key int, value string)");
            System.out.println("创建数据表:" + status);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        HiveJdbcTest test = new HiveJdbcTest();
        test.createStatement();
        //test.createTable("javaTable");
        test.testShowDBs();
    }
}