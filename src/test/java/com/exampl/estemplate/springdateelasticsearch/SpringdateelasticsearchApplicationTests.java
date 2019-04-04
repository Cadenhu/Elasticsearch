package com.exampl.estemplate.springdateelasticsearch;

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringdateelasticsearchApplicationTests {
	private  TransportClient client;
	private final String CLUSTERNAME="myescluster";
	private final Integer PROT=9300;
	@Before
	public void initClent() throws UnknownHostException {
		Settings settings=Settings.builder()
				.put("cluster.name",CLUSTERNAME)
				//.put("client.transport.sniff",true) //启用嗅探
				.build();
		 client=new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),PROT));
	}
	@After
	public void closeClient(){
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
		SearchResponse response= client.search(new SearchRequest("city")).get();
		System.out.println("se>>>>"+response);
	}

	@Test
	public void multiSearchESTest(){
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
		long nbHits=0;
		for (MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			System.out.println("se>>>"+response);
			nbHits += response.getHits().getTotalHits();
		}
		System.out.println(">>>>nbHits="+nbHits);
	}
	//设置index
	@Test
	public  void setESTest(){
		Map<String, Object> json = new HashMap<>();
		json.put("cityName","上海");
		json.put("years",new Date());
		json.put("message","上海市市一个繁华的城市");
		IndexResponse response= client.prepareIndex("city","sh").setSource(json).get();
		System.out.println("in>>>>"+response.status());
	}
	//通过脚本更新操作
	@Test
	public void updateESByScriptTest() throws ExecutionException, InterruptedException {
		UpdateRequest scriptRequest=new UpdateRequest("customer","_doc","3")
				.script(new Script("ctx._source.name=\"胡狼\""));
		UpdateResponse updateResponse= client.update(scriptRequest).get();
		System.out.println("up>>>>"+updateResponse);
	}
	//通过documents更新操作
	@Test
	public void updateESByDocmentTest() throws IOException, ExecutionException, InterruptedException {
		UpdateRequest updateRequest = new UpdateRequest("customer", "_doc", "3")
				.doc(jsonBuilder()
						.startObject()
						.field("gender", "male")
						.field("name","bert.hu")
						.endObject());
		UpdateResponse response= client.update(updateRequest).get();
		System.out.println("up>>>>"+response.getResult());
	}

	//删除
	@Test
	public void deleteESTest() {
		DeleteResponse response = client.prepareDelete("", "", "").get();
		System.out.println("de>>>>" + response);
	}
}
