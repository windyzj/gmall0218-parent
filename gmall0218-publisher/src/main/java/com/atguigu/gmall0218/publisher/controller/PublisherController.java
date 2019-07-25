package com.atguigu.gmall0218.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0218.publisher.bean.Option;
import com.atguigu.gmall0218.publisher.bean.Stat;
import com.atguigu.gmall0218.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    /**
     * 查询各种总数
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date ){
        List<Map> totalList=new ArrayList<>();

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        int dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);


        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        orderAmountMap.put("value",orderAmountTotal);
        totalList.add(orderAmountMap);

        return   JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id")String id , @RequestParam("date") String todayDate ) {
        if (id.equals("dau")) {
            //日活
            Map dauHourTDMap = publisherService.getDauHour(todayDate);
            String yesterdayDate = getYdateString(todayDate);
            Map dauHourYDMap = publisherService.getDauHour(yesterdayDate);

            Map<String, Map> hourMap = new HashMap();
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);

            return JSON.toJSONString(hourMap);
        }else if(id.equals("order_amount")){
            //交易额
            Map orderHourTDMap = publisherService.getOrderAmountHour(todayDate);
            String yesterdayDate = getYdateString(todayDate);
            Map orderHourYDMap = publisherService.getOrderAmountHour(yesterdayDate);

            Map<String, Map> hourMap = new HashMap();
            hourMap.put("today", orderHourTDMap);
            hourMap.put("yesterday", orderHourYDMap);

            return JSON.toJSONString(hourMap);
        }
        return  null;
    }


    private String getYdateString(String todayDate){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String ydateString="";
        try {
            Date tdate = simpleDateFormat.parse(todayDate);
            Date ydate = DateUtils.addDays(tdate, -1);
            ydateString=simpleDateFormat.format(ydate);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ydateString;

    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date")String date, @RequestParam("keyword") String keyword,@RequestParam("startpage") int startpage,@RequestParam("size") int size){

        Map saleDetailMap = publisherService.getSaleDetail(date, keyword, size, startpage);
        Long total =(Long) saleDetailMap.get("total");
        // 把饼图数据做一下 加工整理
        Map ageMap =(Map) saleDetailMap.get("ageMap");
        // 年龄 1 各个年龄count 变成年龄段的count  2 count=> ratio
        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey =(String) entry.getKey();
            Long ageCount = (Long)entry.getValue();
            if(Integer.parseInt(agekey)<20){
                age_20count+=ageCount;
            }else if(Integer.parseInt(agekey)>=20 &&Integer.parseInt(agekey)<=30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }

        }
        Double age_20ratio=0D;
        Double age20_30ratio=0D;
        Double age30_ratio=0D;

        age_20ratio = Math.round(age_20count * 1000D / total) / 10D;
        age20_30ratio = Math.round(age20_30count * 1000D / total) / 10D;
        age30_ratio = Math.round(age30_count * 1000D / total) / 10D;

        List<Option>  ageOptions=new ArrayList<>();
        ageOptions.add(new Option("20岁以下",age_20ratio));
        ageOptions.add(new Option("20岁到30岁",age20_30ratio));
        ageOptions.add(new Option("30岁以上",age30_ratio));
        Stat ageStat = new Stat("年龄占比", ageOptions);

        // 性别占比饼图
        Map genderMap =(Map) saleDetailMap.get("genderMap");

        Long femaleCount = (Long)genderMap.get("F");
        Long maleCount = (Long)genderMap.get("M");

        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;

        List<Option>  genderOptions=new ArrayList<>();
        genderOptions.add(new Option("男",maleRatio));
        genderOptions.add(new Option("女",femaleRatio));
        Stat genderStat = new Stat("性别占比", genderOptions);

        //饼图列表
        List<Stat> statList=new ArrayList<>();
        statList.add(ageStat);
        statList.add(genderStat);


        Map resultMap=new HashMap();
        resultMap.put("total",saleDetailMap.get("total"));
        resultMap.put("stat",statList);
        resultMap.put("detail",saleDetailMap.get("list"));

        return   JSON.toJSONString(resultMap) ;


    }


}
