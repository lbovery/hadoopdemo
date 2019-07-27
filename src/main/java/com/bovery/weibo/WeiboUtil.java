package com.bovery.weibo;

import com.bovery.HBaseDemo;
import com.sun.org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import shaded.parquet.org.slf4j.Logger;
import shaded.parquet.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author lyb
 */
public class WeiboUtil {


    private static final Logger LOGGER = LoggerFactory.getLogger(WeiboUtil.class);

    /**
     * 创建微博
     */
    public static void createWeibo(String uid, String content) throws IOException {
        Table contentT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        Table relationT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_RELATIONS));
        Table boxT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_INBOX));
        long timeMillis = System.currentTimeMillis();
        String rowKey = uid + "_" + timeMillis;
        Put put = new Put(Bytes.toBytes(rowKey));
        String CF_INFO = "info";
        put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("content"), Bytes.toBytes(content));
        //更新微博内容
        contentT.put(put);
        Result result = HBaseDemo.getRowResult(Constant.TABLE_RELATIONS, uid);
        List<Put> puts = Arrays.stream(result.rawCells()).filter(cell -> "fans".equals(Bytes.toString(CellUtil.cloneFamily(cell))))
                .map(cell -> {
                    Put putInbox = new Put(CellUtil.cloneQualifier(cell));
                    putInbox.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
                    return putInbox;
                }).collect(toList());
        boxT.put(puts);
        contentT.close();
        boxT.close();
    }


    public static void addAttend(String uid, String... uids) throws IOException {
        Table contentT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        Table relationT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_RELATIONS));
        Table boxT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_INBOX));
        List<Put> putList = new ArrayList<>();
        Put relationPut = new Put(Bytes.toBytes(uid));
        for (String id : uids) {
            //增加关注
            relationPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(id), Bytes.toBytes(id));
            //被关注人增加粉丝
            Put relationPut1 = new Put(Bytes.toBytes(id));
            relationPut1.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            putList.add(relationPut1);
        }
        putList.add(relationPut);
        relationT.put(putList);
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String id : uids) {
            Scan scan = new Scan(Bytes.toBytes(id), Bytes.toBytes(id + "|"));
            ResultScanner scanner = contentT.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();
                String rowKey = Bytes.toString(row);
                long ts = Long.parseLong(rowKey.split("_")[1]);
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(id), ts, row);
            }
        }
        boxT.put(inboxPut);
        contentT.close();
        relationT.close();
        boxT.close();
    }


    public static void minAttend(String uid, String... uids) throws IOException {
        Table relationT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_RELATIONS));
        Table boxT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_INBOX));
        Delete delete = new Delete(Bytes.toBytes(uid));
        List<Delete> deletes = new ArrayList<>();
        Delete boxDelete = new Delete(Bytes.toBytes(uid));
        for (String ID : uids) {
            delete.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(ID));
            boxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(ID));
            Delete delete1 = new Delete(Bytes.toBytes(ID));
            delete1.addColumns(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deletes.add(delete1);
        }
        deletes.add(delete);
        relationT.delete(deletes);
        boxT.delete(boxDelete);
        relationT.close();
        boxT.close();
    }
    public static void getAllData(String uid, Table contentT) throws IOException {
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        ResultScanner scanner = contentT.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }
        contentT.close();
    }

    public static void getAllData(String uid) throws IOException {
        Table contentT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        getAllData(uid, contentT);
    }


    public static void getPersonData(String uid) throws IOException {
        Table contentT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        Table boxT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_INBOX));
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions();
        List<Get> contentGets = new ArrayList<>();
        Result result = boxT.get(get);
        for (Cell cell : result.rawCells()) {
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            contentGets.add(contentGet);
        }
        Result[] results = contentT.get(contentGets);
        for (Result result1 : results) {
            for (Cell cell : result1.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
