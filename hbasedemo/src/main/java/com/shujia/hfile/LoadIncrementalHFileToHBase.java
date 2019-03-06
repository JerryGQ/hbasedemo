package com.shujia.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 将数据load到hbase
 */
public class LoadIncrementalHFileToHBase {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        HTable table = new HTable(conf,"hua");

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

        loader.doBulkLoad(new Path("/wc/hbase/wc"), table);
    }
}
