package com.expedia.edw.hbase.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.expedia.edw.hbase.constants.HBColumn;

/**
 * HBase bulk import example
 * <p>
 * Parses Hive SequenceFiles and outputs <ImmutableBytesWritable, KeyValue>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it into the correct HBase table region.
 * <p>
 * The KeyValue value holds the HBase mutation information (column family, column, and value)
 */
public class HBaseKVMapper extends Mapper<BytesWritable, Text, ImmutableBytesWritable, KeyValue> {

    private static final String keySeparator = "\u0001";

    final Map<Integer, List<HBColumn>> columns = new HashMap<Integer, List<HBColumn>>();
    final ImmutableBytesWritable hKey = new ImmutableBytesWritable();

    int[] compKeys;
    KeyValue kv;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        // rowkey pattern (1,12,2,4)
        String[] keyFields = conf.get("hbase.table.key").split(",");
        compKeys = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            compKeys[i] = Integer.parseInt(keyFields[i]);
        }

        // column families patterns (usr:0&name,3&age,7&email)
        int cfCount = conf.getInt("hbase.table.key.cf.count", 0);
        for (int i = 0; i < cfCount; i++) {
            String cfPattern = conf.get("hbase.table.cf" + i);

            int ind = cfPattern.indexOf(':');
            String colFamily = cfPattern.substring(0, ind);
            cfPattern = cfPattern.substring(ind + 1, cfPattern.length());
            String[] cfFields = cfPattern.split(",");
            for (String cfField : cfFields) {
                ind = cfField.indexOf('&');
                int pos = Integer.parseInt(cfField.substring(0, ind));
                String colQualifier = cfField.substring(ind + 1, cfField.length());
                if (!columns.containsKey(pos)) {
                    columns.put(pos, new LinkedList<HBColumn>());
                }
                columns.get(pos).add(new HBColumn(colFamily, colQualifier));
            }
        }
    }

    @Override
    protected void map(BytesWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null || value.getLength() == 0) {
            context.getCounter("HBaseKVMapper", "NULL_OR_EMPTY_DATA").increment(1);
            return;
        }
        String[] fields = value.toString().split("\u0001");

        // create rowkey
        StringBuilder strb = new StringBuilder(60);
        for (int i = 0; i < compKeys.length; i++) {
            int pos = compKeys[i];
            if (pos >= fields.length) {
                context.getCounter("HBaseKVMapper", "INVALID_KEY_LEN").increment(1);
                return;
            }
            strb.append(fields[pos]).append((i < compKeys.length - 1) ? keySeparator : "");
        }
        hKey.set(strb.toString().getBytes());

        // fulfill columns
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].isEmpty()) {
                List<HBColumn> colList = columns.get(i);
                if (colList != null) {
                    for (HBColumn hbColumn : colList) {
                        kv = new KeyValue(hKey.get(), hbColumn.getColFamily(), hbColumn.getColQualifier(), fields[i].getBytes());
                        context.write(hKey, kv);
                    }
                }
            }
        }

        context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);
    }
}