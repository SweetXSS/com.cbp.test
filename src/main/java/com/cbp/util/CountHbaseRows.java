package com.cbp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

public class CountHbaseRows {
    public static void main(String[] args) {
        String table_name = args[0];
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "masterhost1, masterhost2, masterhost3");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Scan scan = new Scan();
        AggregationClient aggregationClient = new AggregationClient(hbaseConf);

        try {
            System.out.println("RowCount: " + aggregationClient.rowCount(TableName.valueOf(table_name), new LongColumnInterpreter(), scan));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
