package ct.web.service.impl;

import ct.web.bean.CallLog;
import ct.web.dao.CallLogDao;
import ct.web.service.CallLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

/**
 * 通话日志服务对象
 */

@Service
public class CallLogServiceImpl implements CallLogService {

    @Autowired
    private CallLogDao callLogDao;

    @Override
    public List<CallLog> queryMonthDatas(String tel, String callTime) {

        //查询条件
        HashMap<String, Object> map = new HashMap<>();

        map.put("tel",tel);
        //查询月数据
        if(callTime.length() > 4){
            callTime = callTime.substring(0,4);
        }
        map.put("year",callTime);

        return callLogDao.queryMonthDatas(map);
    }
}
