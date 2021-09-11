package ct.web.controller;

import ct.web.bean.CallLog;
import ct.web.service.CallLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通话日志控制对象
 */

@Controller
public class CallLogController {

    @Autowired
    private CallLogService callLogService;



    /**
     * 查询页面
     * @return
     */
    @RequestMapping("/query")
    public String query(){
        return "query";
    }


    @RequestMapping("/view")
    public Object view(String tel,String callTime,Model model){
       //查询结果统计
        List<CallLog> callLogs = callLogService.queryMonthDatas(tel, callTime);

        //将数据存储到model中
        model.addAttribute("callLogs",callLogs);

        return "view";
    }

}
