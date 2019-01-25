package com.amazon.opendistro.performanceanalyzer.sqlite;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class PerformanceAnalyzerSqliteTest {
    @Test
    public void testTableCreation() {
        //assertFalse(true);
        try {
            String url = "jdbc:sqlite:";
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            // Do some updates
            stmt.executeUpdate("create table sample(id, name)");
            stmt.executeUpdate("insert into sample values(1, \"leo\")");
            stmt.executeUpdate("insert into sample values(2, \"yui\")");
            // Dump the database contents to a file
        }
        catch (Exception e) {
        }
    }

    @Test
    public void testQuery() {
        //assertFalse(true);
        try {
            String url = "jdbc:sqlite:";
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            // Do some updates
            stmt.executeUpdate("create table sample(id, name)");
            stmt.executeUpdate("insert into sample values(1, \"leo\")");
            stmt.executeUpdate("insert into sample values(2, \"yui\")");
            ResultSet r = stmt.executeQuery("select * from sample");
            int count = 0;
            while(r.next()) {
                count++;
            }
            assertEquals(2, count);
        }
        catch (Exception e) {
        }
    }

    @Test
    public void divByZero() {
        //assertFalse(true);
        try {
            String url = "jdbc:sqlite:";
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            // Do some updates
            stmt.executeUpdate("create table sample(id, name)");
            stmt.executeUpdate("insert into sample values(1, \"leo\")");
            stmt.executeUpdate("insert into sample values(2, \"yui\")");
            ResultSet r = stmt.executeQuery("select id/0 from sample");
            while(r.next()) {
                assertEquals(null, r.getBigDecimal(1));
            }
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Got Exception");
        }
    }

    @Test
    public void avgWhenNoRows() {
        //assertFalse(true);
        try {
            String url = "jdbc:sqlite:";
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            // Do some updates
            stmt.executeUpdate("create table sample(id, name)");
            //stmt.executeUpdate("insert into sample values(1, \"leo\")");
            //stmt.executeUpdate("insert into sample values(2, \"yui\")");
            ResultSet r = stmt.executeQuery("select avg(id) from sample");
            while(r.next()) {
                assertEquals(null, r.getBigDecimal(1));
            }
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Got Exception");
        }
    }

    //@Test
    public void pragma() {
        //assertFalse(true);
        try {
            String url = "jdbc:sqlite:";
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            // Do some updates
            stmt.executeUpdate("PRAGMA journal_mode = OFF");
            stmt.executeUpdate("PRAGMA soft_heap_limit = 10000000");
            ResultSet r = stmt.executeQuery("PRAGMA cache_size");
            while(r.next()) {
                System.out.println(r.getBigDecimal(1));
            }
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Got Exception");
        }
    }
}

