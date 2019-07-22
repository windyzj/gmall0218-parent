package com.atguigu.gmall0218.publisher.service;

import java.util.Map;

public interface PublisherService {


    /*
    查询总数
     */
    public int getDauTotal(String date );

    /*
    查询分时明细
     */
    public Map getDauHour(String date);

}
