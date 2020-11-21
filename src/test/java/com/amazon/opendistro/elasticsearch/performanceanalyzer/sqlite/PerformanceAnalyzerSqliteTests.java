/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.sqlite;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Test;

public class PerformanceAnalyzerSqliteTests {

    public PerformanceAnalyzerSqliteTests() {
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            System.setProperty("java.io.tmpdir", "/tmp");
        });
    }


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

