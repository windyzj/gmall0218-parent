package com.atguigu.gmall0218.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {


    public Double getOrderAmountTotal(String date);

    public List<Map> getOrderAmountHour(String date );

}
