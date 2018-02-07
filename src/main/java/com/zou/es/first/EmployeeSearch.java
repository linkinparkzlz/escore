package com.zou.es.first;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 基于Java对员工信息进行搜索
 * <p>
 * 需求：职位中包含technique的员工,age在30-40，分页查询，查找第一页
 */

public class EmployeeSearch {

    @SuppressWarnings({ "unchecked", "resource" })
    public static void main(String[] args) throws Exception {

        //构建client
        Settings setting = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

       // prepareData(client);

        executeSearch(client);

        client.close();

    }


    /**
     * 搜索操作
     */

    private static void executeSearch(TransportClient client) {
        SearchResponse response = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.matchQuery("position", "technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))
                .setFrom(0).setSize(1)
                .get();

        SearchHit[] searchHits = response.getHits().getHits();
        for(int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
        }
    }

    /**
     * 准备数据
     */

    private static void prepareData(TransportClient client) throws Exception {

        IndexResponse response1 = client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "tom")
                        .field("age", 17)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-03-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response1.getResult());


        IndexResponse response2 = client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "mary")
                        .field("age", 17)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-04-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response2.getResult());

        IndexResponse response3 = client.prepareIndex("company", "employee", "3")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "tom")
                        .field("age", 28)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-05-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response3.getResult());

        IndexResponse response4 = client.prepareIndex("company", "employee", "4")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "jack")
                        .field("age", 32)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-03-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response4.getResult());


        IndexResponse response5 = client.prepareIndex("company", "employee", "5")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "tony")
                        .field("age", 35)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-03-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response4.getResult());


    }


}
