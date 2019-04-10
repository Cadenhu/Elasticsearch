/**
 * Copyright (C), 2015-2019
 * FileName: ElasticsearchConfig
 * Author:   huhu
 * Date:     2019/4/5 12:34
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.exampl.estemplate.springdateelasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author huhu
 * @create 2019/4/5
 * @since 1.0.0
 */
@Configuration
public class ElasticsearchConfig {

    @Value("${es.cluster-name}")
    private String clusterName;
    @Value("${es.port}")
    private Integer port;
    @Value("${es.ip}")
    private String host;

    @Bean
    public TransportClient client() throws UnknownHostException {
        Settings settings=Settings.builder()
                .put("cluster.name",clusterName)
                //.put("client.transport.sniff",true) //启用嗅探
                .build();
        TransportClient  client=new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
        return  client;
    }
}