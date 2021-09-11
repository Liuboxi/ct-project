package ct.analysis.tool;

import ct.analysis.io.MySqlTextOutputFormat;
import ct.analysis.mapper.AnalysisTextMapper;
import ct.analysis.reducer.AnalysisTextReducer;
import ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 驱动类
 */
public class AnalysisTextTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1.获取job
        Job job = Job.getInstance();

        //2.设置jar包路径
        job.setJarByClass(AnalysisTextTool.class);

        //3.管理mapper和reducer

        //4.设置map输出的kv类型

        //mapper

        //扫描条件
        Scan scan = new Scan();
        //添加列族信息
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisTextMapper.class,
                Text.class, Text.class,
                job
        );

        //reducer
        job.setReducerClass(AnalysisTextReducer.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //6.设置输入和输出路径   outputformat
        job.setOutputFormatClass(MySqlTextOutputFormat.class);


        //7.提交job
        boolean flag = job.waitForCompletion(true);
        if(flag){
            return JobStatus.State.SUCCEEDED.getValue();
        }else {
            return JobStatus.State.FAILED.getValue();
        }

    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
