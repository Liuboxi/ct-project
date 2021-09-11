package ct.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据reducer
 */
public class AnalysisTextReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;    //通话次数统计
        int sumDuration = 0;    //通话时长统计

        //将每次mapper传入的数据汇总
        for (Text value : values) {

            //单次通话时长
            int duration = Integer.parseInt(value.toString());

            //总通话时长
            sumDuration =  duration + sumDuration;

            //通话次数
            sumCall++;
        }
        //数据形式：（tel_date,sumCall_duration）
        context.write(key,new Text(sumCall + "_" + sumDuration));
    }
}
