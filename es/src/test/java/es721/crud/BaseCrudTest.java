package es721.crud;

import constant.EsConfig;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

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
     *
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
//        client.index(indexRequest, RequestOptions.DEFAULT);

        client.close();
    }

    /**
     * 获取 EsConfig.FIRST_INDEX索引中id = 1 的文档信息
     *
     * @throws IOException .
     */
    @Test
    public void get() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        GetRequest getRequest = GetApi.createGetRequest(EsConfig.FIRST_INDEX, "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Object message = getResponse.getSourceAsString();
        System.out.println(message);
    }

    /**
     * 判断 EsConfig.FIRST_INDEX索引中id = 1 的文档是否存在
     *
     * @throws IOException .
     */
    @Test
    public void exists() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        GetRequest existsRequest = ExistsApi.createExistsRequest(EsConfig.FIRST_INDEX, "1");
        boolean exists = client.exists(existsRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除 EsConfig.FIRST_INDEX索引中id = 1 的文档
     *
     * @throws IOException .
     */
    @Test
    public void delete() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        DeleteRequest deleteRequest = DeleteApi.createDeleteApi(EsConfig.FIRST_INDEX, "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        RestStatus status = deleteResponse.status();
        if (status.getStatus() == 200) {
            System.out.println("删除成功");
        } else {
            throw new RuntimeException("删除失败 ...");
        }
    }

    /**
     * 更新 EsConfig.FIRST_INDEX索引中id = 1 的文档
     *
     * @throws IOException .
     */
    @Test
    public void update() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        UpdateRequest updateRequest = UpdateApi.createUpdateRequest(EsConfig.FIRST_INDEX, "2");
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        RestStatus status = updateResponse.status();
        if (status.getStatus() == 200) {
            // UPDATED or NOOP
            System.out.println(updateResponse.getResult());
            System.out.println("更新成功");
        } else {
            throw new RuntimeException("更新失败 ...");
        }
    }

    /**
     * upserts EsConfig.FIRST_INDEX索引中id = 1 的文档
     * 如果不存在就新增 如果存在就更新
     *
     * @throws IOException .
     */
    @Test
    public void upserts() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        UpdateRequest upsertsRequest = UpdateApi.createUpsertsRequest(EsConfig.FIRST_INDEX, "4");
        UpdateResponse updateResponse = client.update(upsertsRequest, RequestOptions.DEFAULT);
        RestStatus status = updateResponse.status();
        System.out.println(status.getStatus());
        if (status.getStatus() == 200) {
            // UPDATED or NOOP
            System.out.println(updateResponse.getResult());
            System.out.println("更新成功");
        } else if (status.getStatus() == 201) {
            // CREATED
            System.out.println(updateResponse.getResult());
            System.out.println("新增成功");
        } else {
            throw new RuntimeException("upinserts failed ...");
        }

    }

    /**
     * 批量操作
     *
     * @throws IOException .
     */
    @Test
    public void bulk() throws IOException {
        RestHighLevelClient client = CreateApi.createClient();
        BulkRequest bulkRequest = BulkApi.createBulkRequest(EsConfig.FIRST_INDEX);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        // 如果其中有失败的操作
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure =
                            bulkItemResponse.getFailure();
                    System.out.println(failure);
                }
            }
        }

        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
            }
        }
    }

    /**
     * bulkProcessor api要比bulk 复杂一些 不过更加透明
     * 并且可以根据请求的数量或大小，或在给定的时间段之后，自动刷新批量操作
     * <p>
     * warn: setConcurrentRequests()很容易理解错误
     * 设置并发请求数。默认是1，表示允许执行1个并发请求，积累bulk requests和发送bulk这两个操作可以使用不同的线程去执行，
     * 其数值表示发送bulk的并发线程数（可以为2、3、...）；若设置为0表示二者同步。
     * <p>
     * 假如setConcurrentRequests(2) 表示积累bulk requests是一个独立的线程，当到达触发条件时，会再起一个新的线程去发送bulk数据
     * 但是最多只能有两个线程去发送bulk数据
     * <p>
     * warn: 因此当setConcurrentRequests的线程并发数大于1 而直接调用close()方法 就很有可能丢失数据
     * 因为积累bulk requests在内存中进行操作 没有任何的IO 所以速度很快 仅仅一个线程就足够了 而发送bulk的线程
     * 涉及IO操作 速度相对较慢 如果直接调用close()方法 就会产生以下现象
     * 积累bulk requests已经将所有的数据分批完了 等待发送bulk线程来发送数据到ES中
     * 这个时候调用close()方法 很有可能积累bulk requests中的分批数据还有一部分没有来得及处理 因为发送bulk线程处理较慢 从而导致数据丢失
     * 经典现象就是listener中的beforeBulk()方法已经执行 而afterBulk()方法还未执行
     * 但是只要有发送bulk的线程在执行 即使调用close() 该线程中的数据也会发送到ES中
     * 所以只要 setConcurrentRequests > 1 就必须使用awaitClose()
     *
     * <p>
     * 当setConcurrentRequests(0) 就完全是同步操作了 没有任何性能方面的提升
     *
     * @throws InterruptedException .
     */
    @Test
    public void bulkProcessor() throws InterruptedException {
        RestHighLevelClient client = CreateApi.createClient();
        BulkProcessor.Listener listener = BulkApi.createBulkProcessor();

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor bulkProcessor = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(500)
                .setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
                .setConcurrentRequests(30)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                .build();
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < 300000; i++) {
            Map<String, Object> tempMap = new HashMap<>(3);
            tempMap.put("user", "update user" + i);
            tempMap.put("postDate", new Date());
            tempMap.put("message", "trying out Elasticsearch" + i);
            bulkProcessor.add(new IndexRequest(EsConfig.FIRST_INDEX).source(tempMap));
        }
        // setConcurrentRequests(30) 直接使用 close()会丢数据
//        bulkProcessor.close();
        // 如果程序15s就结束了 那么耗时就是15s 而不会一直等到59s
        bulkProcessor.awaitClose(59L, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时:   " + (endTime - beginTime));
    }

    @Test
    public void justTest() throws IOException {
    }


}
