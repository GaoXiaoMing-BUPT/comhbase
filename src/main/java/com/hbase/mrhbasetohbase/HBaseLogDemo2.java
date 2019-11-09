/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/11/9
 * Time: 15:32
 * To change this template use File | Settings | File Templates.
 * Description:
 * 此处需要单独设置打包生产的主类，从HBase读取数据输出到HBase
 * 1.  继承TableMapper
 * 2.  其输入类型已定，为RowKey 和 Result
 * 由于输出到Reducer且最终输出到HBase，故输出key为RowKey，设置value Put（对象），能在mapper做的尽量在mapper做对象构建
 * Reducer尽量少做一些事情，减少Reducer数据合成的工作量
 * 3.   编写run方法
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
            // 构建Put对象  key.get() 或者 value.getRow()
            Put put = new Put(key.get());
            //1. 获取数据
            Cell[] cells = value.rawCells();
            //不同列在不同的循环
            for (Cell cell : cells) {
                double downFlow = 0d;
                double upFlow = 0d;

                //2.    判断当前cell是否为 需要的列 rowKey = phone,columnFamily = info,column = down_flow,column = up_flow
                //2.1   获取downFlow
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("down_flow")) {
                    downFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                }//2.2   获取up_flow
                else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("up_flow")) {
                    upFlow = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                    //3. 给 put 对象赋值 可以直接传cell 求down_flow和up_flow的平均值
                    put.add(cell);
                    //4. 新增列
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("average_flow"), Bytes.toBytes((downFlow + upFlow) / 2 + ""));
                }
            }
            //5. 输出到reducer，输出行数时直接使key相同即可，然后reduce对key进行计数

            context.write(key, put);
        }
    }
    private static class LogReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            //遍历写出
            Put put = new Put(key.get());

            for (Put value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //1. 获取job对象及任务名称
        Job job = Job.getInstance();
        job.setJobName("MapReduce的HBase读写测试");
        //2. 设置主类路径
        job.setJarByClass(HBaseLogDemo2.class);
        //3. 设置mapper类及输入输出类型
        TableMapReduceUtil.initTableMapperJob(SRC_TABLE_NAME, new Scan(), LogMapper.class, ImmutableBytesWritable.class, Put.class, job);
        //4. 设置reducer类及输入输出的表
        TableMapReduceUtil.initTableReducerJob(DST_TABLE_NAME, LogReducer.class, job);
        //5. 由于读写均在HBase，此处不需要设置输入输出路径,此处设置Jar包传入
        job.setJar("out\\artifacts\\comhbase_jar\\comhbase.jar");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    HBaseLogDemo2() {
        //1. hdfs mapreducep配置文件
        //Configuration conf = new Configuration();  //底层已经调用无需调用
        //2. HBase的配置文件
        Configuration conf = HBaseConfiguration.create();
        this.setConf(conf);
    }

    public static void main(String[] args) {
        HBaseDemo.createTable(DST_TABLE_NAME, "info");
        try {
            ToolRunner.run(new HBaseLogDemo2(), args);
        } catch (Exception e) {
            logger.error("运行失败", e);
        }
    }
}
