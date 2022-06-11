package com.r.spark.repl.sql.jdbc;

import java.sql.*;
import java.util.Properties;

public class HiveJdbcClient {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Properties info = new Properties();
            info.setProperty("user","test");
            info.setProperty("password","test");
            //info.setProperty("hiveconf:mapreduce.job.queuename", "root.jichupingtaibu-dashujujiagoubu.offline"); // hive 配置
            //info.setProperty("hivevar:test_var","test_value"); // hive变量，这只是个例子，一般用不到可以删除这一项
            Connection conn = DriverManager.getConnection("jdbc:hive2:/xxxxxx:8088", info);
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("explain select * from gddtest");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            res.close();
            stmt.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
