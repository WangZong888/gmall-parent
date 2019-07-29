package com.wsw.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    //每日活跃总数
    public int selectDauTotal(String date);

    //每日的分时显示
    public List<Map> selectDauTotalHour(String date);
}
