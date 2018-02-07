package com.zou.es.first;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 增删改查程序
 */
public class EmployeeCRUD {

    @SuppressWarnings({"unchecked", "resource"})
    public static void main(String[] args) throws Exception {

        //构建client
        Settings setting = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // createEmployee(client);

        client.close();


    }


    /**
     * 创建一个document
     */

    private static void createEmployee(TransportClient client) throws Exception {

        IndexResponse response = client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("name", "tom")
                        .field("age", 17)
                        .field("position", "hhh")
                        .field("country", "china")
                        .field("join_date", "2017-03-01")
                        .field("salary", "10000").endObject()).get();
        System.out.println(response.getResult());

    }

    /**
     * 获取员工信息
     */

    private static void getEmployee(TransportClient client) throws Exception {

        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getSourceAsBytesRef());
    }


    /**
     * 修改员工信息
     */

    private static void updateEmployee(TransportClient client) throws Exception {
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position", "technique manager")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }


    /**
     * 删除
     */
    public static void deleteEmployee(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("company", "employee", "1").get();
        System.out.println(response.getResult());
    }


}






















