package com.exampl.estemplate.springdateelasticsearch;

import com.exampl.estemplate.springdateelasticsearch.repository.BuildingMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringdateelasticsearchApplicationTests {
    private TransportClient client;
    private final String CLUSTERNAME = "myescluster";
    private final Integer PROT = 9300;


    @Autowired
    private BuildingMapper buildingMapper;
    @Before
    public void initClent() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", CLUSTERNAME)
                //.put("client.transport.sniff",true) //启用嗅探
                .build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), PROT));
    }

    @After
    public void closeClient() {
        client.close();
    }

    //指定index,type,id
    @Test
    public void getESTest() {
        GetResponse response = client.prepareGet("customer", "_doc", "3").get();
        System.out.println(">>>>" + response.getSourceAsString());
    }

    //通过index
    @Test
    public void seachESTest() throws ExecutionException, InterruptedException {
        SearchResponse response = client.search(new SearchRequest("city")).get();
        System.out.println("se>>>>" + response);
    }

    //multi 查询
    @Test
    public void multiSearchESTest() {
        SearchRequestBuilder srb1 = client
                .prepareSearch().setQuery(QueryBuilders
                        .matchQuery("name", "bert.hu"))//全文查询
                .setSize(1);//返回的搜索命中数。默认值为10
        SearchRequestBuilder srb2 = client
                .prepareSearch().setQuery(QueryBuilders
                        .matchQuery("cityName", "上海"))
                .setSize(1);
        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            System.out.println("se>>>" + response);
            nbHits += response.getHits().getTotalHits();
        }
        System.out.println(">>>>nbHits=" + nbHits);
    }

    //设置index
    @Test
    public void setESTest() {
        Map<String, Object> json = new HashMap<>();
        json.put("cityName", "上海");
        json.put("years", new Date());
        json.put("message", "上海市市一个繁华的城市");
        IndexResponse response = client.prepareIndex("city", "sh").setSource(json).get();
        System.out.println("in>>>>" + response.status());
    }

    //通过脚本更新操作
    @Test
    public void updateESByScriptTest() throws ExecutionException, InterruptedException {
        UpdateRequest scriptRequest = new UpdateRequest("customer", "_doc", "3")
                .script(new Script("ctx._source.name=\"胡狼\""));
        UpdateResponse updateResponse = client.update(scriptRequest).get();
        System.out.println("up>>>>" + updateResponse);
    }

    //通过documents更新操作
    @Test
    public void updateESByDocmentTest() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest("customer", "_doc", "3")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .field("name", "bert.hu")
                        .endObject());
        UpdateResponse response = client.update(updateRequest).get();
        System.out.println("up>>>>" + response.getResult());
    }

    //删除
    @Test
    public void deleteESTest() {
        DeleteResponse response = client.prepareDelete("", "", "").get();
        System.out.println("de>>>>" + response);
    }

    //XContentBuilder 创建Index 跟maping
    @Test
    public void createIndexTest() throws IOException {
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate("tospurweb");
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                 .startObject()
                .startObject("properties") //设置之定义字段

                .startObject("id")
                .field("type","integer") //设置数据类型
                .endObject()

                .startObject("building_id")
                .field("type","integer")
                .endObject()

                .startObject("dataln_building_id")
                .field("type","integer")
                .endObject()

                .startObject("dataln_building_unit_id")
                .field("type","integer")
                .endObject()

                .startObject("dataln_presale_id")
                .field("type","integer")
                .endObject()

                .startObject("unit_name")
                .field("type","text")
                .endObject()

                .startObject("layers")
                .field("type","integer")
                .endObject()

                .startObject("sets")
                .field("type","integer")
                .endObject()

                .startObject("tags")
                .field("type","text")
                .endObject()

                .startObject("left_margin")
                .field("type","double")
                .endObject()

                .startObject("top_margin")
                .field("type","double")
                .endObject()

//                .startObject("unit_name")
//                .field("type","string")  //设置Date类型
//                .field("format","yyyy-MM-dd HH:mm:ss") //设置Date的格式
                .endObject()
                .endObject();
        builder.addMapping("building_unit",mapping);
        CreateIndexResponse response= builder.execute().actionGet();
        System.out.println("tospur create>>>" + response);
    }

    //批量导入数据
    @Test
    public void bulkESTest() throws ExecutionException, InterruptedException {
        List<Map<String,Object>> maps= buildingMapper.findAll();

        BulkRequestBuilder bulkRequest=client.prepareBulk();

       // bulkRequest.add(new IndexRequest("tospurweb","building_unit").)
       // client.bulk(bulkRequest);
    }
}
