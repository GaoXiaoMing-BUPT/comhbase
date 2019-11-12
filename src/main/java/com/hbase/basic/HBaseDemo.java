/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/2
 * Time: 16:28
 * To change this template use File | Settings | File Templates.
 * Description:
 * 创建命名空间【删除命名空间（必须无表）】
 * DDL 与 DML 属于严格区分的操作，故对象不一致
 * DDL: 从连接获取管理对象
 * 1.  判断表是否存在
 * 2.  创建表
 * 3.  创建命名空间
 * 4.  删除表
 * DML: 从连接获取表对象
 * 5.  插入数据         生成环境时，利用一个集合去存数据，一定时间间隔或者数量进行写入
 * 6.  查数据（get）
 * 7.  查数据（scan）
 * 8.  删除数据
 * org.apache.hadoop.hbase.util.* 包含大量工具类
 **/
package com.hbase.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseDemo {
    private static final Log logger = LogFactory.getLog(HBaseDemo.class);
    private static final String TABLE_NAME = "ns:student";
    private static final String BASE_INFO = "base_info";
    private static final String EXTRA_INFO = "extra_info";
    private static final String NAME_SPACE = "Beijing";
    private static Connection connection = null;
    private static Admin admin = null;
    private static Configuration conf = null;

    static {
        try {
            //1. 获取配置文件
            conf = HBaseConfiguration.create();
            //2. 获取连接
            connection = ConnectionFactory.createConnection(conf);
            //3. 获取管理对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error("init fail");
        }
    }

    public static void main(String[] args) throws IOException {
        //1. 表存在测试
        logger.info(isTableExist(TABLE_NAME));
        //2. 创建表测试
        createTable(TABLE_NAME, BASE_INFO, EXTRA_INFO);
        //3. 删除表测试
        dropTable(TABLE_NAME);
        //4. 创建命名空间测试
        createNameSpace(NAME_SPACE);
        //5. 命名空间的表创建
        createTable(NAME_SPACE + ":" + "student", BASE_INFO, EXTRA_INFO);
        //6. 删除命名空间
        //7. put数据 一个列族直接添加多个列即可
        putData(NAME_SPACE + ":" + "student", "0001", BASE_INFO, "name", "gaoxiaoming");//若存在 直接覆盖
        putData(NAME_SPACE + ":" + "student", "0001", EXTRA_INFO, "phone", "2019111453");
        //8. 获取单行rowKey 指定列族和列名的数据
        //getData(NAME_SPACE + ":" + "student", "0000", EXTRA_INFO, "phone");
        //8.1 获取rowKey的所有数据
        //getData(NAME_SPACE + ":" + "student", "0000");
        //9. scan获取全部数据测试
        scanTable(NAME_SPACE + ":" + "student");
        //10. 数据删除操作测试
        //deleteData(NAME_SPACE + ":" + "student","0001",BASE_INFO,"name");
        ClusterMetrics clusterMetrics = admin.getClusterMetrics();

        logger.info(clusterMetrics);
        close(admin, connection);

    }

    public static boolean isTableExist(String tableName) throws IOException {
        //1. 获取配置文件信息
        //Configuration configuration = new HBaseConfiguration(); //过时
        //Configuration conf = HBaseConfiguration.create();
        //2. 获取连接
        //Connection connection = ConnectionFactory.createConnection(conf);
        //3. 获取管理对象
        //Admin admin = connection.getAdmin();
        //4. 判断是否存在
        boolean exist = admin.tableExists(TableName.valueOf(tableName));
        //5. 关闭管理对象和连接
//        admin.close();
//        connection.close();
        return exist;
    }

    /* 重要的公共连接 统一管理 */
    public static void close(Admin admin, Connection connection) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                logger.error("admin close fail");
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("connection close fail", e);
            }
        }
    }

    /* 更多的设置信息在builder中 最好不要一次性build */
    public static void createTable(String tableName, String... columnFamily) {
        //判断列族是否为空
        if (columnFamily.length <= 0) {
            logger.warn("columnFamily is null please set info for columnFamily");
            return;
        }

        //判断表是否存在
        try {
            if (isTableExist(tableName)) {
                logger.warn(tableName + " exists,HBase won't create!");
                //return;
            }
        } catch (IOException e) {
            logger.error(e);
        }
        //1. 获取表名
        TableName table = TableName.valueOf(tableName);
        //2. 新建集合存储列族信息
        Collection<ColumnFamilyDescriptor> familiesCollection = new ArrayList<ColumnFamilyDescriptor>();
        //3. 构造列族描述器
        ColumnFamilyDescriptor family;
        for (String cfy : columnFamily) {
            family = ColumnFamilyDescriptorBuilder.newBuilder(cfy.getBytes()).setMaxVersions(5).build();
            familiesCollection.add(family);
        }

        //4. 构造表描述器
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).setColumnFamilies(familiesCollection).build();

        try {
            //5. 新建表
            if (!isTableExist(tableName)) //不存在则新建 否则插入列族即可
                admin.createTable(tableDescriptor);
            else
                logger.info(tableName + " exits, insert columnFamily");

        } catch (IOException e) {
            logger.error(tableName + " creates fail", e);
            return;
        }
    }

    public static void dropTable(String tableName) {
        //1. 判断表是否存在
        try {
            if (!isTableExist(tableName)) {
                logger.warn(tableName + " is not exist");
                return;
            }
            TableName table = TableName.valueOf(tableName);
            //2. 将表下线
            admin.disableTable(table);
            //3. 删除表
            admin.deleteTable(table);
        } catch (IOException e) {
            logger.error(tableName + " drops fail", e);
        }

    }

    public static void createNameSpace(String nameSpace) {

        //1. 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            //2. 创建命名空间
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            logger.warn("namespace: " + nameSpace + " exist, HBase won't create");
        } catch (IOException e) {
            logger.error(nameSpace + " create fail", e);
        }
    }

    public static void deleteNameSpace(String nameSpace) {
        //1. 检测该命名空间是否存在
        try {
            admin.deleteNamespace(nameSpace);
        } catch (NamespaceNotFoundException e) {
            logger.warn(nameSpace + " is not exist");
            return;
        } catch (IOException e) {
            logger.error(nameSpace + " is not empty");
        }
    }

    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) {
        try {
            //1. 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. 创建put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            //3. 个体put对象赋值
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            //4. 插入数据
            table.put(put);
            //5. 回收资源
            table.close();

        } catch (IOException e) {
            logger.error("putData error", e);
        }
    }

    /* 不要使用自带的 cell.getxxx 获取具体的值，会造成乱码 ，应使用CellUtil */
    public static Cell[] getData(String tableName, String rowKey, String columnFamily, String columnName) {
        try {
            //1. 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. 创建get对象
            Get get = new Get(Bytes.toBytes(rowKey));
            //2.1 指定列族和列名
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
            //2.2 指定获取的列族
            //get.addFamily(Bytes.toBytes(columnFamily));
            //2.3 指定获取的版本数
            get.readAllVersions();
            //3. 获取数据
            Result result = table.get(get);
            //4. 解析数据 result
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //5. 输出获取的数据
                logger.info("family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        " columnName: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        " value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );

            }
            //6. 关闭表连接
            table.close();
            return cells;
        } catch (IOException e) {
            logger.error("get data fail", e);
        }
        return null;
    }

    public static Cell[] getData(String tableName, String rowKey) {
        try {
            //1. 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. 新建 get 对象 获取行键值
            Get get = new Get(Bytes.toBytes(rowKey));
            //3. 设置获取的版本数
            get.readAllVersions();
            //4. 获取指定的列族
            //get.addFamily(Bytes.toBytes(columnFamily));
            //5. 获取值
            Result result = table.get(get);
            //6. 解析结果
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                logger.info(
                        " family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                " columnName: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                " value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
            //7. 回收资源
            table.close();

            return cells;
        } catch (IOException e) {
            logger.error(tableName + " put data error", e);
        }

        return null;
    }

    public static ResultScanner scanTable(String tableName) {
        try {
            //1. 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. 获取扫描器对象   可设置过滤器 Filter
            Scan scan = new Scan();
            //3. 获取扫描结果
            ResultScanner results = table.getScanner(scan);
            //4. 解析数据
            for (Result result : results) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    logger.info(
                            " rowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                                    " family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                    " columnName: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                    " value: " + Bytes.toString(CellUtil.cloneValue(cell))
                    );
                }
            }
            //5. 回收资源
            table.close();
            //6. 返回查询的结果集
            return results;
        } catch (IOException e) {
            logger.error("get table object fail", e);
        }
        return null;
    }

    /* 注意区分 Delete 的 addColumn和addColumns的区别 */
    public static void deleteData(String tableName, String rowKey, String columnFamily, String columnName) {
        try {
            //1. 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. 获取删除对象，不进行任何设置则为 rowKey级别的删除即 命令行的 deleteAll
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //2.1 删除指定的列
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));    //删除最新的版本，传入时间戳，则删除指定时间戳  后续版本成为了最新版本
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)); //删除所有版本，传入时间戳时，表示删除小于当前时间戳的所有版本
            //3. 执行删除操作
            table.delete(delete);
            //4. 资源回收
            table.close();

        } catch (IOException e) {
            logger.error(tableName + " delete data fail", e);
        }
    }


}