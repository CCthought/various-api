package es721.crud;

import org.elasticsearch.action.update.UpdateRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhouchengpei
 * date    2019/12/16 10:10
 * description update api 有两种形式 script 和 doc 目前采用doc 因为他比较简单 易于理解
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-update.html#_upserts
 */
public class UpdateApi {

    /**
     * @param index 索引
     * @param id    文档id
     * @return updateRequest Api
     */
    public static UpdateRequest createUpdateRequest(String index, String id) {
        UpdateRequest request = new UpdateRequest(index, id);
        Map<String, Object> jsonMap = new HashMap<>(3);
        jsonMap.put("user", "update user");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        return request.doc(jsonMap);
    }

    /**
     * @param index 索引
     * @param id    文档id
     * @return upsertsRequest Api
     */
    public static UpdateRequest createUpsertsRequest(String index, String id) {
        UpdateRequest request = new UpdateRequest(index, id);
        request.docAsUpsert(true);
        Map<String, Object> jsonMap = new HashMap<>(3);
        jsonMap.put("user", "update user");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        request.upsert(jsonMap);
        request.doc(jsonMap);
        return request;
    }

}
