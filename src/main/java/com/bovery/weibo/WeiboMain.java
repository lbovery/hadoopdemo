package com.bovery.weibo;

import com.bovery.HBaseDemo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * @author lyb
 */

public class WeiboMain {

    @Test
    public void init() throws IOException {
        HBaseDemo.initHBaseConnect();
        HBaseDemo.createNamespace(Constant.NAMESPACE);
        HBaseDemo.createTable(Constant.TABLE_CONTENT, 1,"info");
        HBaseDemo.createTable(Constant.TABLE_RELATIONS,1, "attends", "fans");
        HBaseDemo.createTable(Constant.TABLE_INBOX, 2, "info");
    }

    @Test
    public void test() throws IOException {
        long start = System.currentTimeMillis();
        HBaseDemo.initHBaseConnect();
        long end1 = System.currentTimeMillis();
//        WeiboUtil.createWeibo("1001", "1001的第一条微博");
//        WeiboUtil.createWeibo("1002", "1002的第一条微博");
//        WeiboUtil.addAttend("1001", "1002", "1003");;
//        WeiboUtil.createWeibo("1003", "1003的第2条微博");
//        WeiboUtil.createWeibo("1003", "1003的第3条微博");
//        WeiboUtil.createWeibo("1003", "1003的第4条微博");
        Table contentT = HBaseDemo.getConnection().getTable(TableName.valueOf(Constant.TABLE_CONTENT));
        System.out.println("初始化消耗时间：" +  (end1 - start) /100);
        WeiboUtil.getAllData("1003", contentT);
        long end2 = System.currentTimeMillis();
        System.out.println("查询消耗时间：" +  (end2 - end1) /100);
    }

    public static void main(String[] args) throws IOException{
//        init();
    }
}
