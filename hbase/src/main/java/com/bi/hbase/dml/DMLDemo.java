package com.bi.hbase.dml;

import com.bi.hbase.ddl.DDLDemo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * HBASE的DML语言
 * 给表中 put 数据
 * 给表中 del 数据
 * scan 表中的数据
 * get  表中的数据
 */
public class DMLDemo {
    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop103,hadoop103");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        putData("bi", "student", "1001", "info", "name", "louis");
        scan("bi", "student", "1001", "1003");
        delData("bi", "student", "1001", "info", "name", false);
        getData("bi", "student", "1001", "info", "name");

        putData("ai", "teacher", "1002", "score", "PYTHON", "eagle");
        scan("ai", "teacher", "1002", "1004");
        delData("ai", "teacher", "1002", "score", "JAVA", true);
        getData("ai", "teacher", "1002", "score", "JAVA");
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
     * 向hbase表中增加数据和删除数据
     *
     * @param namespace:
     * @param table:
     * @param rowKey:
     * @param qualifier:
     * @param column:
     * @param value:
     */
    private static void putData(String namespace, String table, String rowKey, String qualifier, String column, String value) throws IOException {
        Table connectionTable = getTable(namespace, table);
        if (connectionTable == null)
            return;

        Put put = new Put(rowKey.getBytes());
        put.addColumn(qualifier.getBytes(), column.getBytes(), value.getBytes());
        connectionTable.put(put);
        connectionTable.close();
    }

    /**
     * 向hbase的表中删除数据
     * @param namespace:
     * @param table:
     * @param rowKey:
     * @param qualifier:
     * @param column:
     * @param isDeleteAll:是否删除当前rowKey下 family 下的column 的所有数据
     * @throws IOException:
     */
    private static void delData(String namespace, String table, String rowKey, String qualifier, String column, boolean isDeleteAll) throws IOException {
        Table connectionTable = getTable(namespace, table);
        if (connectionTable == null)
            return;
        Delete delete = new Delete(rowKey.getBytes());
        if (isDeleteAll) {
            delete.addColumns(qualifier.getBytes(), column.getBytes());
        } else {
            delete.addColumn(qualifier.getBytes(), column.getBytes());
        }
        connectionTable.delete(delete);
        connectionTable.close();
    }

    /**
     * scan 数据
     * @param namespace:
     * @param table:
     * @param startRowKey: 查看rowKey的开始位置
     * @param stopRowKey: 查看rowKey的结束位置
     * @throws IOException:
     */
    private static void scan(String namespace, String table, String startRowKey, String stopRowKey) throws IOException {
        Table connectionTable = getTable(namespace, table);
        if (connectionTable == null)
            return;
        Scan scan = new Scan().withStartRow(startRowKey.getBytes()).withStopRow(stopRowKey.getBytes());
        ResultScanner scanner = connectionTable.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                printCell(cell);
            }
            System.out.println();
        }
        connection.close();
    }

    /**
     * 获取一个rowKey的数据
     * @param namespace:
     * @param table:
     * @param rowKey:
     * @param qualifier:
     * @param column:
     * @throws IOException:
     */
    private static void getData(String namespace, String table, String rowKey, String qualifier, String column) throws IOException {
        Table connectionTable = getTable(namespace, table);
        if (connectionTable == null)
            return;

        Get get = new Get(rowKey.getBytes());
        get.addColumn(qualifier.getBytes(), column.getBytes());
        Result result = connectionTable.get(get);
        for (Cell cell : result.rawCells()) {
            printCell(cell);
        }
        connection.close();
    }

    /**
     * 获取table的连接,若无此表,则返回 null
     * @param namespace：
     * @param table：
     * @return : 返回Table
     * @throws IOException:
     */
    private static Table getTable(String namespace, String table) throws IOException {
        namespace = getNamespace(namespace);
        if (!DDLDemo.isTableExists(namespace, table)) {
            System.out.println(namespace + ":" + table + "\t表不存在");
            return null;
        }
        TableName tableName = TableName.valueOf(namespace.getBytes(), table.getBytes());
        return connection.getTable(tableName);
    }

    private static void printCell(Cell cell) {
        byte[] row = CellUtil.cloneRow(cell);
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        byte[] value = CellUtil.cloneValue(cell);
        System.out.println("row: " + new String(row) + "\tfamily:" + new String(family) +
                "\tqualifier:" + new String(qualifier) + "\tvalue:" + new String(value));
    }

}
