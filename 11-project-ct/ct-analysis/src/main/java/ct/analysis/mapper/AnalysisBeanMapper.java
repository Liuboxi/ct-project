package ct.analysis.mapper;

import ct.analysis.kv.AnalysisKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalysisBeanMapper extends TableMapper<AnalysisKey, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
        //获取rowKey
        String rowKey = Bytes.toString(key.get());

        String[] values = rowKey.split("_");
        String call1 = values[1];
        String call2 = values[3];
        String callTime = values[2];
        String duration = values[4];

        String year = callTime.substring(0, 4);
        String month = callTime.substring(0, 6);
        String day = callTime.substring(0, 8);

        //主叫用户  数据形式：(tel_date,duration)
        //年
        context.write(new AnalysisKey(call1, year), new Text(duration));

        //月
        context.write(new AnalysisKey(call1, month), new Text(duration));

        //日
        context.write(new AnalysisKey(call1, day), new Text(duration));


        //被叫用户
        //年
        context.write(new AnalysisKey(call2, year), new Text(duration));

        //月
        context.write(new AnalysisKey(call2, month), new Text(duration));

        //日
        context.write(new AnalysisKey(call2, day), new Text(duration));


    }
}