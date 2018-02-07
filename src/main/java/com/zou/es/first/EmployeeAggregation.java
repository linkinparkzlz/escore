package com.zou.es.first;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.util.Map;

/**
 * 聚合分析案例
 * 需求：按照country进行分组，在每个分组内，再根据入职年限进行分组，最后计算每个组内的平均薪资
 */


public class EmployeeAggregation {


    public static void main(String[] args) throws Exception {

        //构建client
        Settings setting = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));


        excuteAggregation(client);
        client.close();


    }


    private static void excuteAggregation(TransportClient client) {

        SearchResponse response = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders.dateHistogram("group_by_join_date").field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))))
                .execute().actionGet();

        Map<String, Aggregation> aggregationMap = response.getAggregations().asMap();

        StringTerms groupByCountry = (StringTerms) aggregationMap.get("group_by_country");

        Iterator<Bucket> groupByCountryBucketIterator = groupByCountry.getBuckets().iterator();

        while (groupByCountryBucketIterator.hasNext()) {
            Bucket groupByCountryBucket = groupByCountryBucketIterator.next();
            System.out.println(groupByCountryBucket.getKey() + ":" + groupByCountryBucket.getDocCount());

            Histogram groupByJoinDate = (Histogram) groupByCountryBucket.getAggregations().asMap().get("group_by_join_date");
            Iterator<org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> groupByJoinDateBucketIterator = groupByJoinDate.getBuckets().iterator();
            while (groupByJoinDateBucketIterator.hasNext()) {
                org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket groupByJoinDateBucket = groupByJoinDateBucketIterator.next();
                System.out.println(groupByJoinDateBucket.getKey() + ":" + groupByJoinDateBucket.getDocCount());

                Avg avg = (Avg) groupByJoinDateBucket.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            }
        }


    }


}
//准备数据
//PUT /company
//        {
//        "mappings": {
//        "employee": {
//        "properties": {
//        "age": {
//        "type": "long"
//        },
//        "country": {
//        "type": "text",
//        "fields": {
//        "keyword": {
//        "type": "keyword",
//        "ignore_above": 256
//        }
//        },
//        "fielddata": true
//        },
//        "join_date": {
//        "type": "date"
//        },
//        "name": {
//        "type": "text",
//        "fields": {
//        "keyword": {
//        "type": "keyword",
//        "ignore_above": 256
//        }
//        }
//        },
//        "position": {
//        "type": "text",
//        "fields": {
//        "keyword": {
//        "type": "keyword",
//        "ignore_above": 256
//        }
//        }
//        },
//        "salary": {
//        "type": "long"
//        }
//        }
//        }
//        }
//        }