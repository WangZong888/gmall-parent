package com.wsw.gmall.publisher.servicer.servicerimpl;

import com.wsw.gmall.publisher.mapper.DauMapper;
import com.wsw.gmall.publisher.mapper.OrderMapper;
import com.wsw.gmall.publisher.servicer.DauServicer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-22 12:51
 **/
@Service
public class DauServicerimpl implements DauServicer {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public int getDauTotal(String date) {

        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHour(String date) {
        List<Map> maps = dauMapper.selectDauTotalHour(date);
        //把List<Map>转换成Map
        Map dauHourMap = new HashMap();
        for (Map map : maps) {
            String lh = (String) map.get("LH");
            Long ct = (Long) map.get("CT");
            dauHourMap.put(lh, ct);
        }

        return dauHourMap;
    }


    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //调整map结构
        List<Map> orderAmountHour = orderMapper.getOrderAmountHour(date);
        Map orderAmountMap = new HashMap();
        for (Map map : orderAmountHour) {
            orderAmountMap.put(map.get("CH"),map.get("ST"));
        }
        return orderAmountMap;
    }
}
