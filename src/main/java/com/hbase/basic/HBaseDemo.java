/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/2
 * Time: 16:28
 * To change this template use File | Settings | File Templates.
 * Description:
 * ���������ռ䡾ɾ�������ռ䣨�����ޱ���
 * DDL �� DML �����ϸ����ֵĲ������ʶ���һ��
 * DDL: �����ӻ�ȡ�������
 * 1.  �жϱ��Ƿ����
 * 2.  ������
 * 3.  ���������ռ�
 * 4.  ɾ����
 * DML: �����ӻ�ȡ�����
 * 5.  ��������         ���ɻ���ʱ������һ������ȥ�����ݣ�һ��ʱ����������������д��
 * 6.  �����ݣ�get��
 * 7.  �����ݣ�scan��
 * 8.  ɾ������
 * org.apache.hadoop.hbase.util.* ��������������
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
            //1. ��ȡ�����ļ�
            conf = HBaseConfiguration.create();
            //2. ��ȡ����
            connection = ConnectionFactory.createConnection(conf);
            //3. ��ȡ�������
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error("init fail");
        }
    }

    public static void main(String[] args) throws IOException {
        //1. ����ڲ���
        logger.info(isTableExist(TABLE_NAME));
        //2. ���������
        createTable(TABLE_NAME, BASE_INFO, EXTRA_INFO);
        //3. ɾ�������
        dropTable(TABLE_NAME);
        //4. ���������ռ����
        createNameSpace(NAME_SPACE);
        //5. �����ռ�ı���
        createTable(NAME_SPACE + ":" + "student", BASE_INFO, EXTRA_INFO);
        //6. ɾ�������ռ�
        //7. put���� һ������ֱ����Ӷ���м���
        putData(NAME_SPACE + ":" + "student", "0001", BASE_INFO, "name", "gaoxiaoming");//������ ֱ�Ӹ���
        putData(NAME_SPACE + ":" + "student", "0001", EXTRA_INFO, "phone", "2019111453");
        //8. ��ȡ����rowKey ָ�����������������
        //getData(NAME_SPACE + ":" + "student", "0000", EXTRA_INFO, "phone");
        //8.1 ��ȡrowKey����������
        //getData(NAME_SPACE + ":" + "student", "0000");
        //9. scan��ȡȫ�����ݲ���
        scanTable(NAME_SPACE + ":" + "student");
        //10. ����ɾ����������
        //deleteData(NAME_SPACE + ":" + "student","0001",BASE_INFO,"name");
        ClusterMetrics clusterMetrics = admin.getClusterMetrics();

        logger.info(clusterMetrics);
        close(admin, connection);

    }

    public static boolean isTableExist(String tableName) throws IOException {
        //1. ��ȡ�����ļ���Ϣ
        //Configuration configuration = new HBaseConfiguration(); //��ʱ
        //Configuration conf = HBaseConfiguration.create();
        //2. ��ȡ����
        //Connection connection = ConnectionFactory.createConnection(conf);
        //3. ��ȡ�������
        //Admin admin = connection.getAdmin();
        //4. �ж��Ƿ����
        boolean exist = admin.tableExists(TableName.valueOf(tableName));
        //5. �رչ�����������
//        admin.close();
//        connection.close();
        return exist;
    }

    /* ��Ҫ�Ĺ������� ͳһ���� */
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

    /* �����������Ϣ��builder�� ��ò�Ҫһ����build */
    public static void createTable(String tableName, String... columnFamily) {
        //�ж������Ƿ�Ϊ��
        if (columnFamily.length <= 0) {
            logger.warn("columnFamily is null please set info for columnFamily");
            return;
        }

        //�жϱ��Ƿ����
        try {
            if (isTableExist(tableName)) {
                logger.warn(tableName + " exists,HBase won't create!");
                //return;
            }
        } catch (IOException e) {
            logger.error(e);
        }
        //1. ��ȡ����
        TableName table = TableName.valueOf(tableName);
        //2. �½����ϴ洢������Ϣ
        Collection<ColumnFamilyDescriptor> familiesCollection = new ArrayList<ColumnFamilyDescriptor>();
        //3. ��������������
        ColumnFamilyDescriptor family;
        for (String cfy : columnFamily) {
            family = ColumnFamilyDescriptorBuilder.newBuilder(cfy.getBytes()).setMaxVersions(5).build();
            familiesCollection.add(family);
        }

        //4. �����������
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).setColumnFamilies(familiesCollection).build();

        try {
            //5. �½���
            if (!isTableExist(tableName)) //���������½� ����������弴��
                admin.createTable(tableDescriptor);
            else
                logger.info(tableName + " exits, insert columnFamily");

        } catch (IOException e) {
            logger.error(tableName + " creates fail", e);
            return;
        }
    }

    public static void dropTable(String tableName) {
        //1. �жϱ��Ƿ����
        try {
            if (!isTableExist(tableName)) {
                logger.warn(tableName + " is not exist");
                return;
            }
            TableName table = TableName.valueOf(tableName);
            //2. ��������
            admin.disableTable(table);
            //3. ɾ����
            admin.deleteTable(table);
        } catch (IOException e) {
            logger.error(tableName + " drops fail", e);
        }

    }

    public static void createNameSpace(String nameSpace) {

        //1. ���������ռ�������
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            //2. ���������ռ�
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            logger.warn("namespace: " + nameSpace + " exist, HBase won't create");
        } catch (IOException e) {
            logger.error(nameSpace + " create fail", e);
        }
    }

    public static void deleteNameSpace(String nameSpace) {
        //1. ���������ռ��Ƿ����
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
            //1. ��ȡ�����
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. ����put����
            Put put = new Put(Bytes.toBytes(rowKey));
            //3. ����put����ֵ
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            //4. ��������
            table.put(put);
            //5. ������Դ
            table.close();

        } catch (IOException e) {
            logger.error("putData error", e);
        }
    }

    /* ��Ҫʹ���Դ��� cell.getxxx ��ȡ�����ֵ����������� ��Ӧʹ��CellUtil */
    public static Cell[] getData(String tableName, String rowKey, String columnFamily, String columnName) {
        try {
            //1. ��ȡ�����
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. ����get����
            Get get = new Get(Bytes.toBytes(rowKey));
            //2.1 ָ�����������
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
            //2.2 ָ����ȡ������
            //get.addFamily(Bytes.toBytes(columnFamily));
            //2.3 ָ����ȡ�İ汾��
            get.readAllVersions();
            //3. ��ȡ����
            Result result = table.get(get);
            //4. �������� result
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //5. �����ȡ������
                logger.info("family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        " columnName: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        " value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );

            }
            //6. �رձ�����
            table.close();
            return cells;
        } catch (IOException e) {
            logger.error("get data fail", e);
        }
        return null;
    }

    public static Cell[] getData(String tableName, String rowKey) {
        try {
            //1. ��ȡ�����
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. �½� get ���� ��ȡ�м�ֵ
            Get get = new Get(Bytes.toBytes(rowKey));
            //3. ���û�ȡ�İ汾��
            get.readAllVersions();
            //4. ��ȡָ��������
            //get.addFamily(Bytes.toBytes(columnFamily));
            //5. ��ȡֵ
            Result result = table.get(get);
            //6. �������
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                logger.info(
                        " family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                " columnName: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                " value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
            //7. ������Դ
            table.close();

            return cells;
        } catch (IOException e) {
            logger.error(tableName + " put data error", e);
        }

        return null;
    }

    public static ResultScanner scanTable(String tableName) {
        try {
            //1. ��ȡ�����
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. ��ȡɨ��������   �����ù����� Filter
            Scan scan = new Scan();
            //3. ��ȡɨ����
            ResultScanner results = table.getScanner(scan);
            //4. ��������
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
            //5. ������Դ
            table.close();
            //6. ���ز�ѯ�Ľ����
            return results;
        } catch (IOException e) {
            logger.error("get table object fail", e);
        }
        return null;
    }

    /* ע������ Delete �� addColumn��addColumns������ */
    public static void deleteData(String tableName, String rowKey, String columnFamily, String columnName) {
        try {
            //1. ��ȡ�����
            Table table = connection.getTable(TableName.valueOf(tableName));
            //2. ��ȡɾ�����󣬲������κ�������Ϊ rowKey�����ɾ���� �����е� deleteAll
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //2.1 ɾ��ָ������
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));    //ɾ�����µİ汾������ʱ�������ɾ��ָ��ʱ���  �����汾��Ϊ�����°汾
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)); //ɾ�����а汾������ʱ���ʱ����ʾɾ��С�ڵ�ǰʱ��������а汾
            //3. ִ��ɾ������
            table.delete(delete);
            //4. ��Դ����
            table.close();

        } catch (IOException e) {
            logger.error(tableName + " delete data fail", e);
        }
    }


}