/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/6
 * Time: 17:42
 * To change this template use File | Settings | File Templates.
 * Description:
 * ���ı���ȡ����Ȼ��д��HBase
 * �˴���Ҫ�������ô�����������࣬���ı���ȡ���������HBase
 * 1.   ����map��ȡ����
 * 2.   �̳�HTableReducer
 * 3.   �������ͣ����һ���Ѿ�д����Mutation��д��HBase
 * 4.   д��HBase��Keyû��ϵ���������ֱ��д�ռ���
 * 5.   �޸�Key��ֱ�Ӵ�ȫ�ֵ�������context��ȡ  ��дsetup��cleanup����
 * 6.   ��run�м���hdfs��yarn�������ļ�������conf.addResource ����HBase�������ļ�
 **/
package com.hbase.mrfiletohbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseLogDemo1 extends Configured implements Tool {
    private static String rowKey = null;
    private static final String TABLE_NAME = "log";
    private static Configuration conf = null;
    private static final Log logger = LogFactory.getLog(HBaseLogDemo1.class);
    private Configuration configuration = null;
    private static class LogMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    private static class LogReducer extends TableReducer<LongWritable, Text, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            rowKey = conf.get("cf1");
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //1. ����ÿһ��values
            for (Text value : values) {
                //2. ����ÿһ������
                String[] fields = value.toString().split(" ");
                //3. ����Put����
                Put put = new Put(Bytes.toBytes(fields[0]));
                //4. ��put����д������

                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("down_flow"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("up_flow"), Bytes.toBytes(fields[2]));
                //5. ���ݷ�װ��һ��put������
                context.write(NullWritable.get(),put);
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        //1. ��ȡjob����
        Job job = Job.getInstance();
        job.setJobName("����Log���ļ�д��HBase");
        //2. ����������·��
        job.setJarByClass(HBaseLogDemo1.class);
        //3. ����jar�ļ�
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");
        //3. ����Mapper�༰���KV����
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4. ����Reducer�� ����TableReducer.class����
        //job.setReducerClass(LogReducer.class);  ��ԭMapReduce����
        TableMapReduceUtil.initTableReducerJob(strings[1],LogReducer.class,job);
        //5. ��������������ݵ�KV����  �Ѿ��������������ã��˴�����ʡ��

        //6. ���������������  �������·���Ѿ�����HBase�ı���
        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        //8. �ύ����
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }
    HBaseLogDemo1(){
        //1. ��ȡhdfs yarn�������ļ�
        //configuration = new Configuration();
        //2. ��ȡHBase�������ļ�
        configuration.addResource(HBaseConfiguration.create());// �ײ��Ѿ������� hdfs MapReduce�����ö�ȡ
        //3. ������Ϣ����ToolRunner�������
        this.setConf(configuration);
        //4. �����Ƿ��ȡ��������Ϣ
        logger.info("������Ϣ��" + this.getConf());
    }
    public static void main(String[] args) {
        //1. �������
        String[] strings = new String[]{"/hbase_file",TABLE_NAME};
        //2. ��ȡhdfs yarn�����ļ�
        //Configuration conf = new Configuration();
        //3. ��ȡHBase�����ļ�
        //conf.addResource(HBaseConfiguration.create());
        try {
            //4. ����ToolRunner.run ����
            //ToolRunner.run(conf,new HBaseMR(),strings);
            ToolRunner.run(new HBaseLogDemo1(),strings);
        } catch (Exception e) {
            logger.error("run error",e);
        }

    }
}
