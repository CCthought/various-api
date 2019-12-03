package es721.createclient;

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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouchengpei
 * date   2019/12/3 21:46
 * description  创建索引并创建文档
 */
public class CreateClient {

    /*public static void main(String[] args) throws IOException {
        CreateClient createClient = new CreateClient();
        RestHighLevelClient client = createClient.createClient();
        IndexRequest request = createClient.indexRequestByMap();
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        client.close();
    }*/

    public static void main(String[] args) throws IOException {
        CreateClient createClient = new CreateClient();
        RestHighLevelClient client = createClient.createClient();
        CreateIndexRequest index = createClient.createIndex();
        client.indices().create(index, RequestOptions.DEFAULT);
        client.close();
    }

    /**
     * 创建索引
     *
     * @return 索引
     */
    private CreateIndexRequest createIndex() {
        CreateIndexRequest request = new CreateIndexRequest("twitter_two");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2));
        request.alias(
                new Alias("twitter_alias")
        );
        return request;
    }

    /**
     * 创建一个 es 高级客户端
     *
     * @return es 高级客户端
     */
    private RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(EsConfig.ES_URL, EsConfig.ES_OPEN_PORT, "http")));
    }

    /**
     * 最普通的方式，使用String字符串拼接json，构成一个文档
     */
    private IndexRequest indexRequestByString() {
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
    private IndexRequest indexRequestByMap() {
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
    private IndexRequest indexRequestByBuilder() throws IOException {
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
