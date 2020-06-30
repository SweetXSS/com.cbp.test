package com.cbp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class GetCols {
    public static void main(String[] args) {

    }
    public static ArrayList<String> getcolname(String rowkey, String tablename, String colf) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Get get = new Get(Bytes.toBytes(rowkey));
        ArrayList<String> cols = new ArrayList<String>();
        Table table = conn.getTable(TableName.valueOf(tablename));
        Result result = table.get(get);
        Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(colf));
        for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
            cols.add(Bytes.toString(entry.getKey()));
            System.out.println(Bytes.toString(entry.getKey()));
        }
        conn.close();
        return cols;
    }
}
