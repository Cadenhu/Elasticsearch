package com.exampl.estemplate.springdateelasticsearch;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.exampl.estemplate.springdateelasticsearch.repository.BuildingMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.*;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;
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
import java.util.concurrent.TimeUnit;

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


    @Test
    public void craeteChinaIndexTest() {
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate("china");
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties") //设置之定义字段

                    .startObject("cityName")
                    .field("type", "text")
                    //.field("analyzer", "ik_max_word") //设置ik 分词器
                    .endObject()

                    .startObject("message")
                    .field("type", "text")
                   // .field("analyzer", "ik_max_word")
                    .endObject()
                    .startObject("years")
                    .field("type", "date")  //设置Date类型
                    .field("format", "yyyy-MM-dd HH:mm:ss") //设置Date的格式
                    .endObject()

                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        builder.addMapping("city", mapping);
        CreateIndexResponse response = builder.execute().actionGet();
        System.out.println("tospur create>>>" + response);
    }

    //设置index
    @Test
    public void setChinaIndexTest() {

        Map<String, Object> json = new HashMap<>();
        json.put("cityName", "北京");
        json.put("years", DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        json.put("message", "北京市是一个雾霾的城市");
        Map<String, Object> json2 = new HashMap<>();
        json2.put("cityName", "上海");
        json2.put("years", DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        json2.put("message", "上海市一个繁华的城市");
        IndexResponse response1 = client.prepareIndex("china", "city").setSource(json).get();
        IndexResponse response2 = client.prepareIndex("china", "city").setSource(json2).get();
        //System.out.println("in>>>>" + response.status());
    }

    //prepareSearch 检索数据
    @Test
    public void getChinaIndexTest() throws ExecutionException, InterruptedException {
        SearchResponse searchResponse= client
                .prepareSearch("china").setQuery(QueryBuilders.matchQuery("message","繁")).get(); //.search(searchRequest).get();
        SearchHit[] searchHits= searchResponse.getHits().getHits();
        JSONArray jsonArray=new JSONArray();
        for (SearchHit hit: searchHits) {
            jsonArray.add(JSONObject.parse(hit.getSourceAsString()));
        }
        String json=jsonArray.toJSONString();
        System.out.println(">>>>"+json);
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
                .field("type", "integer") //设置数据类型
                .endObject()

                .startObject("building_id")
                .field("type", "integer")
                .endObject()

                .startObject("dataln_building_id")
                .field("type", "integer")
                .endObject()

                .startObject("dataln_building_unit_id")
                .field("type", "integer")
                .endObject()

                .startObject("dataln_presale_id")
                .field("type", "integer")
                .endObject()

                .startObject("unit_name")
                .field("type", "text")
                .endObject()

                .startObject("layers")
                .field("type", "integer")
                .endObject()

                .startObject("sets")
                .field("type", "integer")
                .endObject()

                .startObject("tags")
                .field("type", "text")
                .endObject()

                .startObject("left_margin")
                .field("type", "double")
                .endObject()

                .startObject("top_margin")
                .field("type", "double")
                .endObject()

//                .startObject("unit_name")
//                .field("type","string")  //设置Date类型
//                .field("format","yyyy-MM-dd HH:mm:ss") //设置Date的格式
                .endObject()
                .endObject();
        builder.addMapping("building_unit", mapping);
        CreateIndexResponse response = builder.execute().actionGet();
        System.out.println("tospur create>>>" + response);
    }

    //批量导入数据,循环每次导入
    @Test
    public void bulkESTest() throws ExecutionException, InterruptedException {
        List<Map<String, Object>> maps = buildingMapper.findAll();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Map<String, Object> map :
                maps) {
            bulkRequest.add(client.prepareIndex("tospurweb", "building_unit")
                    .setSource(map));
        }
        BulkResponse responses = bulkRequest.get();
        System.out.println("bulk>>>>>" + responses.getIngestTookInMillis());
    }

    @Test
    public void searchBuildingTest(){
        SearchResponse searchResponse= client.prepareSearch("tospurweb")
                .setSize(30)
                .setFrom(0)//213750
                .setQuery(QueryBuilders.matchQuery("unit_name","露香园")).get();
        System.out.println(">>>查询用时："+searchResponse.getTook().getMillis());
        System.out.println(">>>查询总记录数："+searchResponse.getHits().getTotalHits());
        SearchHit[]  searchHits= searchResponse.getHits().getHits();
        JSONArray jsonArray=new JSONArray();
        for (SearchHit hit: searchHits) {
            //hit.getScore() 获取匹配分数
           jsonArray.add(JSONObject.parse(hit.getSourceAsString()));
        }
        System.out.println(">>>size="+jsonArray.size()+""+jsonArray.toString());
    }
    //批处理
    @Test
    public void bulkProcessor() throws InterruptedException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client //①添加您的Elasticsearch客户端
                , new BulkProcessor.Listener() {
                    //②在批量执行之前调用此方法
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    //③批量执行后调用此方法
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    }

                    //④批量失败并引发a时调用此方法 Throwable
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    }
                })
                .setBulkActions(10000)//⑤每10000个请求执行批量处理/默认值是1000
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))//⑥每5mb提交一次/默认5mb
                .setFlushInterval(TimeValue.timeValueSeconds(5))//⑦无论请求数量多少，我们都希望每隔5秒刷新一次/默认无
                .setConcurrentRequests(1)//⑧设置并发请求数。值为0表示只允许执行单个请求。值1表示允许执行1个并发请求，同时累积新的批量请求/默认1
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))//⑨Set a custom backoff policy which will initially wait for 100ms, increase exponentially and retries up to three times. A retry is attempted whenever one or more bulk item requests have failed with an EsRejectedExecutionException which indicates that there were too little compute resources available for processing the request. To disable backoff, pass BackoffPolicy.noBackoff()/默认值重试次数为8次，启动延迟为50ms。总等待时间约为5.1秒。
                .build();

        bulkProcessor.add(new IndexRequest("tospurweb", "building_unit").source(new HashMap()));
        bulkProcessor.flush();//刷新剩余的请求
        bulkProcessor.awaitClose(10, TimeUnit.MICROSECONDS);//awaitClose方法将等待所有批量请求完成的指定超时true，如果在所有批量请求完成之前经过指定的等待时间， false则返回/close方法不会等待任何剩余的批量请求完成并立即退出

    }
}
