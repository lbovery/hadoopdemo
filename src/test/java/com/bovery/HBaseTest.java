package com.bovery;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;

/**
 * @author lyb
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseTest {

    private static String tableName;

    private static String columnFamily;

    @BeforeClass
    public static void init() throws IOException {
        HBaseDemo.initHBaseConnect();
        tableName = "station_data";
        columnFamily = "info";
    }

    @Test
    public void test1() throws IOException {
        HBaseDemo.createTable(tableName, columnFamily);
    }



    @Test
    public void test2() throws IOException {
        HBaseDemo.addRowData(tableName, columnFamily, "10001", "pm", "70");
        HBaseDemo.addRowData(tableName, columnFamily, "10001", "noise", "50");
        HBaseDemo.addRowData(tableName, columnFamily, "10002", "pm", "32.5");
        HBaseDemo.addRowData(tableName, columnFamily, "10002", "noise", "53");
        HBaseDemo.addRowData(tableName, columnFamily, "10003", "pm", "45");
        HBaseDemo.addRowData(tableName, columnFamily, "10003", "noise", "43");
    }

    @Test
    public void test3() throws IOException {
        HBaseDemo.getAllRows(tableName);
    }

    @Test
    public void test4() throws IOException {
        HBaseDemo.deleteMultiRow(tableName, "10001");
    }

    @Test
    public void test5() throws IOException {
        HBaseDemo.getRow(tableName, "10002");
    }

    @Test
    public void test6() throws IOException {
        HBaseDemo.getRowQualifier(tableName, "10002", columnFamily, "pm");
    }

    @Test
    public void test7() throws IOException {
        HBaseDemo.getRowName(tableName);
    }


    @Test
    public void test8() throws IOException {
        HBaseDemo.dropTable(tableName);
    }



}
