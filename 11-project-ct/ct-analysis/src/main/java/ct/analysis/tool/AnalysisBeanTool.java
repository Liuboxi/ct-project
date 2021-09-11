package ct.analysis.tool;
import ct.analysis.io.MySqlBeanOutputFormat;
import ct.analysis.kv.AnalysisKey;
import ct.analysis.kv.AnalysisValue;
import ct.analysis.mapper.AnalysisBeanMapper;
import ct.analysis.reducer.AnalysisBeanReducer;
import ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class AnalysisBeanTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1.获取job
        Job job = Job.getInstance();

        //2.设置jar包路径
        job.setJarByClass(AnalysisBeanTool.class);

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
                AnalysisBeanMapper.class,
                AnalysisKey.class, Text.class,
                job
        );

        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        //6.设置输入和输出路径   outputformat
        job.setOutputFormatClass(MySqlBeanOutputFormat.class);


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
