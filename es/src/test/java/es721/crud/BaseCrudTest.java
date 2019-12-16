package es721.crud;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zhouchengpei
 * date   2019/12/3 22:44
 * description .
 */
public class BaseCrudTest {

    /**
     * 1.创建一个ES高级客户端
     * 2.创建一个索引 first-index，别名first-index-alias，设置3个主分片，2个副本
     * 3.向first-index索引插入一个文档
     * @throws IOException .
     */
    @Test
    public void create() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        CreateIndexRequest index = CreateApi.createIndex();
        // 创建索引
        client.indices().create(index, RequestOptions.DEFAULT);
        IndexRequest indexRequest = CreateApi.createIndexRequestByMap();
        // 插入文档信息
        client.index(indexRequest, RequestOptions.DEFAULT);
        client.close();
    }

    @Test
    public void get() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        GetRequest getRequest = GetApi.createGetRequest();
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Object message = getResponse.getField("message");
        System.out.println(message);
        System.out.println(getResponse.getSourceAsMap());
    }


}
