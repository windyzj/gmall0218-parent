package com.atguigu.gmall0218.publisher.service.impl;

import com.atguigu.gmall0218.common.constant.GmallConstant;
import com.atguigu.gmall0218.publisher.mapper.DauMapper;
import com.atguigu.gmall0218.publisher.mapper.OrderMapper;
import com.atguigu.gmall0218.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        int dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourList = dauMapper.getDauHour(date);
        //把 List<Map> 转换成  Map
        Map duaHourMap=new HashMap();
        for (Map map : dauHourList) {
            String loghour =(String ) map.get("LOGHOUR");
            Long ct =(Long ) map.get("CT");
            duaHourMap.put(loghour,ct);

        }
        return duaHourMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //调整一下map结构
        List<Map> orderAmountHourList = orderMapper.getOrderAmountHour(date);

        Map orderAmountMap=new HashMap();
        for (Map map : orderAmountHourList) {
            orderAmountMap.put( map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }

        return orderAmountMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyword, int pageSize, int pageNo ) {


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);
        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);
       //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);
        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE_DETAIL).addType("_doc").build();
        Map resultMap=new HashMap();  //需要总数， 明细，2个聚合的结果
        try {
            SearchResult searchResult = jestClient.execute(search);
            //总数
            Long total = searchResult.getTotal();

            //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map> saleDetailList=new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add(hit.source) ;
            }
            //年龄聚合结果
            Map ageMap=new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                ageMap.put(bucket.getKey(),bucket.getCount());
            }
            //性别聚合结果
            Map genderMap=new HashMap();
            List<TermsAggregation.Entry> genderbuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            for (TermsAggregation.Entry bucket : genderbuckets) {
                genderMap.put(bucket.getKey(),bucket.getCount());
            }

            resultMap.put("total",total);
            resultMap.put("list",saleDetailList);
            resultMap.put("ageMap",ageMap);
            resultMap.put("genderMap",genderMap);




        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }


}
