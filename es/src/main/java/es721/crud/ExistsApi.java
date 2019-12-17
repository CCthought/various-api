package es721.crud;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

/**
 * @author zhouchengpei
 * date    2019/12/16 9:28
 * description exists api
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-exists.html
 */
public class ExistsApi {

    /**
     * @param index 索引
     * @param id    文档id
     * @return getRequest exists Api
     */
    public static GetRequest createExistsRequest(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return getRequest;
    }

}
