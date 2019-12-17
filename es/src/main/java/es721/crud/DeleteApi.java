package es721.crud;

import constant.EsConfig;
import org.elasticsearch.action.delete.DeleteRequest;

/**
 * @author zhouchengpei
 * date    2019/12/16 9:43
 * description delete api
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.2/java-rest-high-document-delete.html
 */
public class DeleteApi {

    /**
     * @param index 索引
     * @param id    文档id
     * @return deleteRequest Api
     */
    public static DeleteRequest createDeleteApi(String index, String id) {
        return new DeleteRequest(EsConfig.FIRST_INDEX, "1");
    }

}
