package com.expedia.edw.hbase.jobs;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.expedia.edw.hbase.mappers.HBaseKVMapper;

/**
 * HBase bulk import from Hive Sequence file<br>
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * <li>args[3]: HBase table key pattern col4,col1,...,coln (0,4,2,5)
 * <li>args[4]: HBase table column family pattern cf1:col1&col_name,...,coln&col_name (usr:0&name,3&age,7&email)
 * <li>args[n]: HBase table column family pattern cfn:col1&col_name,...,coln&col_name (log:2&key,0&name,5&pass)
 * </ol>
 */
public class HbaseBulkImportJob extends Configured implements Tool {

    private static final boolean isDebug = true;

    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HbaseBulkImportJob(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (isDebug) {
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
            conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        }

        if (args.length < 5) {
            throw new ConfigurationException("Incorrect number of params");
        } else if (!args[2].matches("\\w+")) {
            throw new ConfigurationException("Bad HBase table name");
        } else if (!args[3].matches("^(\\d+,)+\\d$")) {
            throw new ConfigurationException("Bad HBase rowkey pattern");
        }

        conf.set("hbase.table.name", args[2]);
        conf.set("hbase.table.key", args[3]);
        conf.setInt("hbase.table.key.cf.count", args.length - 4);
        for (int i = 4, j = 0; i < args.length; i++, j++) {
            if (!args[i].matches("\\w+:([0-9]+&\\w+,)+[0-9]+&\\w+")) {
                throw new ConfigurationException("Bad HBase column family " + i + " pattern");
            }

            conf.set("hbase.table.cf" + j, args[i]);
        }

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf, "HBase Bulk Import for " + args[2]);
        job.setJarByClass(HbaseBulkImportJob.class);
        job.setMapperClass(HBaseKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Auto configure partitioner and reducer
        HTable hTable = new HTable(conf, args[2]);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int exitCode = 1;
        if (job.waitForCompletion(true)) {
            exitCode = 0;

            // Load generated HFiles into table
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(new Path(args[1]), hTable);
        }

        return exitCode;
    }
}