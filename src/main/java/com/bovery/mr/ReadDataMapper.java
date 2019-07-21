package com.bovery.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lyb
 */
public class ReadDataMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        //遍历添加column行
        for (Cell cell : value.rawCells()) {
            //添加/克隆列族:info
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                //添加/克隆列：name
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    //将该列cell加入到put对象中
                    put.add(cell);
                    //添加/克隆列:color
                } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    //向该列cell加入到put对象中
                    put.add(cell);
                }
            }
        }
        //将读取到的每行数据写入到context中作为map的输出
        context.write(key, put);
    }
}
