package com.wsw.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.wsw.gmall.publisher.servicer.DauServicer;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-22 23:04
 **/
@RestController
public class PublisherController {

    @Autowired
    DauServicer dauServicer;

    @GetMapping("test")
    public String test(){
        System.out.println("处理日活");
        return "test";
    }
    //查询各种总数
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date")String date){
        List<Map> totalList = new ArrayList<>();

        //日活总数
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","每天活跃");
        int dauTotal = dauServicer.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        //新增用户
        Map newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",345);
        totalList.add(newMidMap);

        //新增交易额
        Map orderAmount = new HashMap();
        orderAmount.put("id","order_amount");
        orderAmount.put("name","新增交易额");
        Double orderAmountTotal = dauServicer.getOrderAmountTotal(date);
        orderAmount.put("value",orderAmountTotal);
        totalList.add(orderAmount);


        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id")String id,@RequestParam("date")String todayDate) {

        if("dau".equals(id)){
            //日活
            Map todayTotalHours = dauServicer.getDauTotalHour(todayDate);
            String yesterday = getYesterday(todayDate);
            Map yestDayTotalHours = dauServicer.getDauTotalHour(yesterday);

            //将map<lh,ct>放在map<s,m>中
            Map<String,Map> hourMap = new HashMap<>();
            hourMap.put("today",todayTotalHours);
            hourMap.put("yesterday",yestDayTotalHours);
            return JSON.toJSONString(hourMap);
            }else if("order_amount".equals(id)){
                //交易额
                Map todayTotalHours = dauServicer.getOrderAmountHour(todayDate);
                String yesterday = getYesterday(todayDate);
                Map yestDayTotalHours = dauServicer.getOrderAmountHour(yesterday);

                //将map<lh,ct>放在map<s,m>中
                Map<String,Map> hourMap = new HashMap<>();
                hourMap.put("today",todayTotalHours);
                hourMap.put("yesterday",yestDayTotalHours);
                return JSON.toJSONString(hourMap);
            }
        return null;
    }

    //获取昨天的时间
    private String getYesterday(String todayDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String yesterdayStr = "";
        try {
            Date tdate = sdf.parse(todayDate);
            Date ydate = DateUtils.addDays(tdate, -1);
            yesterdayStr = sdf.format(ydate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterdayStr;

    }
}
