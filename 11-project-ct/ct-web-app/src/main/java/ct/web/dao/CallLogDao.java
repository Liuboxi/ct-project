package ct.web.dao;

import ct.web.bean.CallLog;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * 通话日志数据访问对象
 */

@Repository
public interface CallLogDao {
    //查询每个月的通话记录
    public List<CallLog> queryMonthDatas(Map<String,Object> map);
}
