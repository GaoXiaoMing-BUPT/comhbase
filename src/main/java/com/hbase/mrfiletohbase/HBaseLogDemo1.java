/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/6
 * Time: 17:42
 * To change this template use File | Settings | File Templates.
 * Description:
 * 从文本读取数据然后写入HBase
 * 此处需要单独设置打包生产的主类，从文本读取数据输出到HBase
 * 1.   正常map读取数据
 * 2.   继承HTableReducer
 * 3.   三个泛型，最后一个已经写死（Mutation）写入HBase
 * 4.   写入HBase跟Key没关系，正常情况直接写空即可
 * 5.   修改Key，直接从全局的上下文context读取  重写setup和cleanup即可
 * 6.   在run中加入hdfs和yarn的配置文件，利用conf.addResource 加入HBase的配置文件
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
            //1. 处理每一个values
            for (Text value : values) {
                //2. 分离每一行数据
                String[] fields = value.toString().split(" ");
                //3. 构建Put对象
                Put put = new Put(Bytes.toBytes(fields[0]));
                //4. 给put对象写入数据

                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("down_flow"), Bytes.toBytes(fields[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("up_flow"), Bytes.toBytes(fields[2]));
                //5. 数据封装成一个put对象传入
                context.write(NullWritable.get(),put);
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        //1. 获取job对象
        Job job = Job.getInstance();
        job.setJobName("测试Log的文件写入HBase");
        //2. 设置驱动类路径
        job.setJarByClass(HBaseLogDemo1.class);
        //3. 加载jar文件
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");
        //3. 设置Mapper类及输出KV类型
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4. 设置Reducer类 设置TableReducer.class即可
        //job.setReducerClass(LogReducer.class);  与原MapReduce区别
        TableMapReduceUtil.initTableReducerJob(strings[1],LogReducer.class,job);
        //5. 设置最终输出数据的KV类型  已经设置了上述设置，此处可以省略

        //6. 设置输入输出参数  由于输出路径已经定向到HBase的表中
        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        //8. 提交任务
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }
    HBaseLogDemo1(){
        //1. 获取hdfs yarn的配置文件
        //configuration = new Configuration();
        //2. 获取HBase的配置文件
        configuration.addResource(HBaseConfiguration.create());// 底层已经调用了 hdfs MapReduce的配置读取
        //3. 配置信息传入ToolRunner供其调用
        this.setConf(configuration);
        //4. 测试是否获取到配置信息
        logger.info("配置信息：" + this.getConf());
    }
    public static void main(String[] args) {
        //1. 传入参数
        String[] strings = new String[]{"/hbase_file",TABLE_NAME};
        //2. 获取hdfs yarn配置文件
        //Configuration conf = new Configuration();
        //3. 获取HBase配置文件
        //conf.addResource(HBaseConfiguration.create());
        try {
            //4. 利用ToolRunner.run 运行
            //ToolRunner.run(conf,new HBaseMR(),strings);
            ToolRunner.run(new HBaseLogDemo1(),strings);
        } catch (Exception e) {
            logger.error("run error",e);
        }

    }
}
