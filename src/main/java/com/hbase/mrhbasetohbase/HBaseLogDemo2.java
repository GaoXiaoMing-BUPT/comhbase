/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/9
 * Time: 15:32
 * To change this template use File | Settings | File Templates.
 * Description:
 * �˴���Ҫ�������ô�����������࣬��HBase��ȡ���������HBase
 * 1.  �̳�TableMapper
 * 2.  �����������Ѷ���ΪRowKey �� Result
 * ���������Reducer�����������HBase�������keyΪRowKey������value Put�����󣩣�����mapper���ľ�����mapper�����󹹽�
 * Reducer��������һЩ���飬����Reducer���ݺϳɵĹ�����
 * 3.   ��дrun����
 **/
package com.hbase.mrhbasetohbase;

import com.hbase.basic.HBaseDemo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;

public class HBaseLogDemo2 extends Configured implements Tool {
    private static final String SRC_TABLE_NAME = "log";
    private static final String DST_TABLE_NAME = "log_out_test";
    private static final Log logger = LogFactory.getLog(HBaseLogDemo2.class);

    private static class LogMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // ����Put����  key.get() ���� value.getRow()
            Put put = new Put(key.get());
            //1. ��ȡ����
            Cell[] cells = value.rawCells();
            //��ͬ���ڲ�ͬ��ѭ��
            for (Cell cell : cells) {
                double downFlow = 0d;
                double upFlow = 0d;

                //2.    �жϵ�ǰcell�Ƿ�Ϊ ��Ҫ���� rowKey = phone,columnFamily = info,column = down_flow,column = up_flow
                //2.1   ��ȡdownFlow
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("down_flow")) {
                    downFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }//2.2   ��ȡup_flow
                else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("up_flow")) {
                    upFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                    //3. �� put ����ֵ ����ֱ�Ӵ�cell ��down_flow��up_flow��ƽ��ֵ
                    put.add(cell);
                    //4. ������
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("average_flow"), Bytes.toBytes((downFlow + upFlow) / 2 + ""));
                }
            }
            //5. �����reducer���������ʱֱ��ʹkey��ͬ���ɣ�Ȼ��reduce��key���м���

            context.write(key, put);
        }
    }
    private static class LogReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            //����д��
            Put put = new Put(key.get());

            for (Put value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //1. ��ȡjob������������
        Job job = Job.getInstance();
        job.setJobName("MapReduce��HBase��д����");
        //2. ��������·��
        job.setJarByClass(HBaseLogDemo2.class);
        //3. ����mapper�༰�����������
        TableMapReduceUtil.initTableMapperJob(SRC_TABLE_NAME, new Scan(), LogMapper.class, ImmutableBytesWritable.class, Put.class, job);
        //4. ����reducer�༰��������ı�
        TableMapReduceUtil.initTableReducerJob(DST_TABLE_NAME, LogReducer.class, job);
        //5. ���ڶ�д����HBase���˴�����Ҫ�����������·��,�˴�����Jar������
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    HBaseLogDemo2() {
        //1. hdfs mapreducep�����ļ�
        //Configuration conf = new Configuration();  //�ײ��Ѿ������������
        //2. HBase�������ļ�
        Configuration conf = HBaseConfiguration.create();
        this.setConf(conf);
    }

    public static void main(String[] args) {
        HBaseDemo.createTable(DST_TABLE_NAME, "info");
        try {
            ToolRunner.run(new HBaseLogDemo2(), args);
        } catch (Exception e) {
            logger.error("����ʧ��", e);
        }
    }
}
