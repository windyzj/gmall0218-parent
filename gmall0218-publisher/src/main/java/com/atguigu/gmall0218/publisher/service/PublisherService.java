package com.atguigu.gmall0218.publisher.service;

import java.util.Map;

public interface PublisherService {


    /*
    查询日活总数
     */
    public int getDauTotal(String date );

    /*
    查询日活分时明细
     */
    public Map getDauHour(String date);

    /*
     查询总交易额
     */
    public Double getOrderAmountTotal(String date);

    /*
    查询分时交易额
     */
    public Map getOrderAmountHour(String date);


}
