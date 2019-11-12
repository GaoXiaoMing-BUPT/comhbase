/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/9
 * Time: 16:55
 * To change this template use File | Settings | File Templates.
 * Description:
 * 此处需要单独设置打包生产的主类,读取HBase数据输出到文件
 * 1.  从HBase中读取值
 * 2.  mapper输入无需指定，输出为Text和Null
 * 3.  reducer直接输出到文件
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
            //1. 获取每一个rowKey的值
            Cell[] cells = value.rawCells();
            double downFlow = 0d;
            double upFlow = 0d;
            double averageFlow = 0d;
            for (Cell cell : cells) {
                //2. 获取downFlow
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("down_flow")) {
                    downFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }//3. 获取upFlow值
                else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("up_flow")) {
                    upFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                    averageFlow = (downFlow + upFlow) / 2;
                }
            }
            //4. 写入值
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
        //1. 获取job对象
        Job job = Job.getInstance();
        //2. 设置任务名称
        job.setJobName("HBase读出数据，MapReduce处理后写入hdfs");
        //3. 设置主类
        job.setJarByClass(HBaseLogDemo3.class);
        //4. 加载jar文件
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");
        //5. 设置mapper及输入输出  此处读取全部数据不进行过滤
        TableMapReduceUtil.initTableMapperJob(SRC_TABLE_NAME, new Scan(), LogMapper.class, Text.class, NullWritable.class, job);
        //6. 设置reducer
        job.setReducerClass(LogReducer.class);
        //7. 设置输入输入出的类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //8. 设置输出文件
        FileOutputFormat.setOutputPath(job, new Path("/HbaseOutTest"));
        //9. 提交运行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private HBaseLogDemo3() {
        //1. 读取 HDFS MapReduce HBase 的配置文件
        Configuration configuration = HBaseConfiguration.create();
        //2. 传入参数
        this.setConf(configuration);
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new HBaseLogDemo3(), args);
        } catch (Exception e) {
            logger.error("运行失败", e);
        }
    }
}
