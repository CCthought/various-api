package es721.crud;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouchengpei
 * date    2019/12/16 11:22
 * description bulk api
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-bulk.html#CO94-2
 */
public class BulkApi {

    /**
     * 批量处理api
     * warn:The Bulk API supports only documents encoded in JSON or SMILE.
     * Providing documents in any other format will result in an error.
     * <p>
     * 不同操作类型(index create update delete)都可以放到一个 BulkRequest中
     *
     * @return bulkRequest api
     */
    public static BulkRequest createBulkRequest(String index) {
        Map<String, Object> jsonMap = new HashMap<>(3);
        jsonMap.put("user", "update user");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");

        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(index, "4"));
        request.add(new UpdateRequest(index, "2")
                .doc(jsonMap));
        request.add(new IndexRequest(index).id("5")
                .source(jsonMap));
        return request;
    }

    /**
     * 异步批量处理api
     *
     * @return BulkProcessor
     */
    public static BulkProcessor.Listener createBulkProcessor() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                System.out.println("id" + executionId + " 执行的个数 " + numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    System.out.println(String.format("Bulk [%s] executed with failures", executionId));
                } else {
                    System.out.println(String.format("Bulk [%s] completed in %s milliseconds",
                            executionId, response.getTook().getMillis()));
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                System.out.println("Failed to execute bulk");
                System.out.println(failure.getMessage());
            }
        };
    }

}
