package es721.crud;

import constant.EsConfig;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

/**
 * @author zhouchengpei
 * date   2019/12/4 14:56
 * description get API
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-get.html
 */
public class GetApi {

    /**
     * @return getRequest Api
     */
    public static GetRequest createGetRequest() {
        GetRequest getRequest = new GetRequest(EsConfig.FIRST_INDEX, "1");


        getRequest.storedFields("message");
//        getRequest.fetchSourceContext(new FetchSourceContext(true,new String[]{"postDate","message","user"},
//                        new String[]{"postDate"}));
        return getRequest;
    }

}
