package com.shujia.read;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyMapper extends TableMapper<NullWritable, Text> {

    /**
     * Mapper
     */
    public void map(ImmutableBytesWritable rows, Result result, Context context) throws IOException, InterruptedException {
        //把取到的值直接打印
        for (Cell kv : result.listCells()) { // 遍历每一行的各列
            //假如我们当时插入HBase的时候没有把int、float等类型的数据转换成String，这里就会乱码了
            String row = new String(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), "UTF-8");
            //获取列族
            String family = new String(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), "UTF-8");
            //获取列名
            String qualifier = new String(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(), "UTF-8");
            //获取value值
            String value = new String(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), "UTF-8");

            Text text = new Text(row + "\t" + value);
            context.write(NullWritable.get(),text);

        }
    }

}
