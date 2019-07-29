package com.wsw.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //一天的总金额
    public Double getOrderAmountTotal(String date);
    //每个小时的总金额
    public List<Map> getOrderAmountHour(String date);

}
