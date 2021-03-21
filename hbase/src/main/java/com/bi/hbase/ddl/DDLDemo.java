package com.bi.hbase.ddl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

/**
 * HBase的DDL语言
 * 主要功能:
 *  增加namespace
 *  增加table
 *  删除table
 *  判断table是否存在
 */
public class DDLDemo {
    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        createNamespace("bi");
        createTable("bi", "student","info", "score");
        deleteTable("bi", "student");
    }

    /**
     * 创建库
     *
     * @param namespace:
     * @throws IOException:
     */
    public static void createNamespace(String namespace) throws IOException {
        if (namespace == null || namespace.length() < 1) {
            System.out.println("库名不合法");
        }
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        NamespaceDescriptor build = builder.build();
        try {
            admin.createNamespace(build);
            System.out.println("建库成功");
        } catch (IOException e) {
            System.out.println("库已经存在");
        } finally {
            admin.close();
        }
    }

    /**
     * 内部方法,若库名为 null 或为 ""
     * 则将库名转化为default
     *
     * @param namespace:
     * @return :
     */
    private static String getNamespace(String namespace) {
        return namespace == null || namespace.length() < 1 ? "default" : namespace;
    }

    /**
     * 判断库中表是否存在
     *
     * @param namespace:
     * @param table:
     * @return :
     * @throws IOException:
     */
    public static boolean isTableExists(String namespace, String table) throws IOException {
        if (table == null || table.length() < 1) {
            System.out.println("表名格式错误");
            return false;
        }
        namespace = getNamespace(namespace);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(namespace.getBytes(), table.getBytes());
        boolean tableExists = admin.tableExists(tableName);
        admin.close();
        return tableExists;
    }

    /**
     * 建表
     *
     * @param namespace:
     * @param table:
     * @param qualifiers:
     * @throws IOException:
     */
    public static void createTable(String namespace, String table, String... qualifiers) throws IOException {
        if (isTableExists(namespace, table)) {
            System.out.println("表已经存在,不能创建表");
        } else if(table == null || table.length() < 1) {
            System.out.println("表名不能为空");
            return;
        }
        namespace = getNamespace(namespace);
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace.getBytes(), table.getBytes()));

        for (String qualifier : qualifiers) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(qualifier.getBytes());
            ColumnFamilyDescriptor familyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        TableDescriptor build = tableDescriptorBuilder.build();
        admin.createTable(build);

        admin.close();

    }

    /**
     * 删除表
     *
     * @param namespace: 库名
     * @param table:     表名
     * @throws IOException:
     */
    public static void deleteTable(String namespace, String table) throws IOException {
        if (!isTableExists(namespace, table)) {
            System.out.println("表不存在,不能删除");
            return;
        }else if(table == null || table.length() < 1) {
            System.out.println("被删除表的表名不能为空");
            return;
        }
        namespace = getNamespace(namespace);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(namespace.getBytes(), table.getBytes());
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }
}
