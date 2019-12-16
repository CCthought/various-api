package es721.crud;

import constant.EsConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouchengpei
 * date   2019/12/3 21:46
 * description  1.创建ES高级客户端 2.创建索引 3.插入文档信息
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-index.html
 */
public class CreateApi {

    /**
     * 创建索引
     *
     * @return 索引
     */
    public static CreateIndexRequest createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(EsConfig.FIRST_INDEX);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2));
        request.alias(
                new Alias(EsConfig.FIRST_INDEX_ALIAS)
        );
        return request;
    }

    /**
     * 创建一个 es 高级客户端
     *
     * @return es 高级客户端
     */
    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(EsConfig.ES_URL, EsConfig.ES_OPEN_PORT, "http")));
    }

    /**
     * 最普通的方式，使用String字符串拼接json，构成一个文档
     */
    public static IndexRequest createIndexRequestByString() {
        IndexRequest request = new IndexRequest("first-index");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        return request;
    }

    /**
     * Document source provided as a Map which gets automatically
     * converted to JSON format
     */
    public static IndexRequest createIndexRequestByMap() {
        Map<String, Object> jsonMap = new HashMap<>(3);
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        return new IndexRequest("first-index")
                .id("1").source(jsonMap);
    }

    /**
     * Document source provided as an XContentBuilder object,
     * the Elasticsearch built-in helpers to generate JSON content
     *
     * @throws IOException .
     */
    public static IndexRequest createIndexRequestByBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        return new IndexRequest("posts")
                .id("1").source(builder);
    }

}
