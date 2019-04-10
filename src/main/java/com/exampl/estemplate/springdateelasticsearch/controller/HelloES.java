/**
 * Copyright (C), 2015-2019
 * FileName: HelloES
 * Author:   huhu
 * Date:     2019/4/5 12:39
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.exampl.estemplate.springdateelasticsearch.controller;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author huhu
 * @create 2019/4/5
 * @since 1.0.0
 */
@RestController
public class HelloES {

    @Autowired
    public TransportClient client;
    @GetMapping("/get")
    @ResponseBody
    public Map<String, Object> getById(String id) {
        GetResponse response = client.prepareGet("city", "sh", id).get();
        return response.getSource();
    }
}