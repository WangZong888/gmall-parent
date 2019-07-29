package com.wsw.gmall.publisher.servicer;

import java.util.Map;

public interface DauServicer {

    /*
     查询日活
     */
    public int getDauTotal(String date);

    /*
     查询日活分时明细
     */
    public Map getDauTotalHour(String date);

    /*
       查询总交易额
     */
    public Double getOrderAmountTotal(String date);

    /*
        查询分时交易额
     */
    public Map getOrderAmountHour(String date);
}
