package ct.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String date;

    public AnalysisKey(){

    }

    public AnalysisKey(String tel,String date){
        this.tel = tel;
        this.date = date;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    /**
     * 比较
     * @param analysisKey
     * @return
     */
    @Override
    public int compareTo(AnalysisKey analysisKey) {
        int result = tel.compareTo(analysisKey.getTel());
        if(result == 0){
            result = date.compareTo(analysisKey.getDate());
        }
        return result;
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeUTF(date);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
         tel = dataInput.readUTF();
         date = dataInput.readUTF();
    }
}
