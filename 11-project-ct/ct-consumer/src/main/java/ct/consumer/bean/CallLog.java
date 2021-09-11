package ct.consumer.bean;

import ct.common.annotation.Column;
import ct.common.annotation.RowKey;
import ct.common.annotation.TableRef;

/**
 * 通话日志
 */

@TableRef("ct:calllog")
public class CallLog {
    @RowKey
    private String rowKey;

    @Column(family = "caller")
    private String call1;

    @Column(family = "caller")
    private String call2;

    @Column(family = "caller")
    private String callTime;

    @Column(family = "caller")
    private String duration;

    @Column(family = "caller")
    private String flag = "1";  //1代表主叫，0代表被叫。

    private String name;

    /**
     * 将获取的字符串数据分割成独立的属性
     * @param data
     */
    public CallLog(String data){
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        callTime = values[2];
        duration = values[3];
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
