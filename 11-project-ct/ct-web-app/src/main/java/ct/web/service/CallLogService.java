package ct.web.service;

import ct.web.bean.CallLog;
import java.util.List;

public interface CallLogService {
    //查询每个月的通话记录
    public List<CallLog> queryMonthDatas(String tel,String callTime);
}
