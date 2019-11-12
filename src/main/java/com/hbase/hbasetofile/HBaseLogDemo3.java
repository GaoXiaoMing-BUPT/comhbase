/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/9
 * Time: 16:55
 * To change this template use File | Settings | File Templates.
 * Description:
 * �˴���Ҫ�������ô������������,��ȡHBase����������ļ�
 * 1.  ��HBase�ж�ȡֵ
 * 2.  mapper��������ָ�������ΪText��Null
 * 3.  reducerֱ��������ļ�
 **/
package com.hbase.hbasetofile;

import com.hbase.mrhbasetohbase.HBaseLogDemo2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseLogDemo3 extends Configured implements Tool {
    private static final String SRC_TABLE_NAME = "log";
    private static final String DST_TABLE_NAME = "log_out_test";
    private static final Log logger = LogFactory.getLog(HBaseLogDemo3.class);


    private static class LogMapper extends TableMapper<Text, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //1. ��ȡÿһ��rowKey��ֵ
            Cell[] cells = value.rawCells();
            double downFlow = 0d;
            double upFlow = 0d;
            double averageFlow = 0d;
            for (Cell cell : cells) {
                //2. ��ȡdownFlow
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("down_flow")) {
                    downFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }//3. ��ȡupFlowֵ
                else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("up_flow")) {
                    upFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                    averageFlow = (downFlow + upFlow) / 2;
                }
            }
            //4. д��ֵ
            context.write(new Text(String.format("%.2f\t%.2f\t%.2f", downFlow, upFlow, averageFlow)), NullWritable.get());
        }
    }

    private static class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, value);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //1. ��ȡjob����
        Job job = Job.getInstance();
        //2. ������������
        job.setJobName("HBase�������ݣ�MapReduce�����д��hdfs");
        //3. ��������
        job.setJarByClass(HBaseLogDemo3.class);
        //4. ����jar�ļ�
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");
        //5. ����mapper���������  �˴���ȡȫ�����ݲ����й���
        TableMapReduceUtil.initTableMapperJob(SRC_TABLE_NAME, new Scan(), LogMapper.class, Text.class, NullWritable.class, job);
        //6. ����reducer
        job.setReducerClass(LogReducer.class);
        //7. �����������������
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //8. ��������ļ�
        FileOutputFormat.setOutputPath(job, new Path("/HbaseOutTest"));
        //9. �ύ����
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private HBaseLogDemo3() {
        //1. ��ȡ HDFS MapReduce HBase �������ļ�
        Configuration configuration = HBaseConfiguration.create();
        //2. �������
        this.setConf(configuration);
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new HBaseLogDemo3(), args);
        } catch (Exception e) {
            logger.error("����ʧ��", e);
        }
    }
}
