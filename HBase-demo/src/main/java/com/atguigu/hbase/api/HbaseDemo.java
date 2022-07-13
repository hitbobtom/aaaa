package com.atguigu.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase API
 */
public class HbaseDemo {

    public static void main(String[] args) throws IOException {
        //System.out.println(connection);
        testCreateTable(null , "emp" , "f1", "f2");

        connection.close();
    }

    /**
     *  DDL - 创建表
     */
    public static void testCreateTable(String namespaceName , String tableName , String ... cfs ) throws IOException {
        //基本的判空操作
        Admin admin = connection.getAdmin();


        //判断表是否已经存在
        TableName tn = TableName.valueOf(namespaceName, tableName);
        boolean exists = admin.tableExists(tn);
        if(exists){
            System.err.println((namespaceName==null ? "default":namespaceName) + ":" + tableName + " 表已经存在!");
            return ;
        }

        //建表
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        //设置列族
        if(cfs.length <= 0 ){
            System.err.println("至少指定一个列族");
            return ;
        }
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
        System.out.println((namespaceName==null ? "default":namespaceName) + ":" + tableName + " 创建成功!");
        admin.close();
    }



    /**
     *  Connection
     */
    public static Connection connection ;

    static{
        try {
            Configuration configuration = new Configuration();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
