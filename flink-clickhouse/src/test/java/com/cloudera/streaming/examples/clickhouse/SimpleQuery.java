package com.cloudera.streaming.examples.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 */
public class SimpleQuery {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:clickhouse://clickhouse-dev:9001";
        String user = "root";
        String password = "Ch@ngy0u.com";
        Connection connection = DriverManager.getConnection(url, user, password);

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT (number % 3 + 1) as n, sum(number) FROM numbers(10000000) GROUP BY n");

        while (rs.next()) {
            System.out.println(rs.getInt(1) + "\t" + rs.getLong(2));
        }
    }
}