package com.bovery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lyb
 */
public class HBaseDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDemo.class);

    private static Connection connection;

    public static Connection getConnection() {
        return connection;
    }

    public static void setConnection(Connection connection) {
        HBaseDemo.connection = connection;
    }


    /**
     * 初始化配置 这里注意将集群配置文件复制到resource目录下
     * @throws IOException
     */
    public static void initHBaseConnect() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
    }

    //创建命名空间
    public static void createNamespace(String name) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    public static void createTable(String tableName, int version, String... columnFamily) throws IOException {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(version);
                descriptor.addFamily(columnDescriptor);
            }
            //根据对表的配置，创建表
            Admin admin = connection.getAdmin();
            admin.createTable(descriptor);
            admin.close();
            LOGGER.info("表 [{}] 创建成功！", tableName);
        }
    }

    public static void createTable(String tableName, String... columnFamily) throws IOException {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            Admin admin = connection.getAdmin();
            admin.createTable(descriptor);
            admin.close();
            LOGGER.info("表 [{}] 创建成功！", tableName);
        }
    }

    private static boolean isTableExist(String tableName) throws IOException {
        return connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    public static void dropTable(String tableName) throws IOException {
        if (isTableExist(tableName)) {
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            LOGGER.info("表 [{}] 删除成功！", tableName);
            admin.close();
        } else {
            LOGGER.warn("表 [{}] 不存在！", tableName);
        }
    }

    /**
     * 插入数据  如果rowKey存在则更新
     *
     * @param tableName    表名
     * @param rowKey       rowKey
     * @param columnFamily CF
     * @param column       列
     * @param value        值
     */
    public static void addRowData(String tableName, String columnFamily, String rowKey, String column, String value) throws IOException {
        //创建HTable对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        LOGGER.info("插入数据成功");
    }

    /**
     * 删除多列数据
     *
     * @param tableName 表明
     * @param rows      rowKeyList
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }


    public static void getAllRows(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(result -> {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowKey
                LOGGER.info("行键: [{}]", Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                LOGGER.info("列族:列 == [{}:{}] = [{}]", Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        });
    }

    /**
     * 获取指定行数据
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        Result result = getRowResult(tableName, rowKey);
        for (Cell cell : result.rawCells()) {
            LOGGER.info("列族:列:行键 == [{}:{}:{}] = [{}], [{}]", Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(result.getRow()), Bytes.toString(CellUtil.cloneValue(cell)), cell.getTimestamp());
        }
    }

    public static Result getRowResult(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        return table.get(get);

    }

    /**
     * 获取某一行指定“列族:列”的数据
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            LOGGER.info("列族:列:行键 == [{}:{}:{}] = [{}]", Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(result.getRow()), Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void getRowName(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<String> list = new ArrayList<>();
        HTableDescriptor hTableDescriptor = table.getTableDescriptor();
        for (HColumnDescriptor hColumnDescriptor : hTableDescriptor.getColumnFamilies()) {
            list.add(hColumnDescriptor.getNameAsString());
        }
        LOGGER.info(list.toString());
    }


}
