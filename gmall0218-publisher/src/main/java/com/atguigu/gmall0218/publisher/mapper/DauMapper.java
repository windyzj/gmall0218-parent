package com.atguigu.gmall0218.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    public int  getDauTotal(String date);

    public List<Map>   getDauHour(String date);

}
