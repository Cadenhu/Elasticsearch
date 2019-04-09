package com.exampl.estemplate.springdateelasticsearch.repository;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface BuildingMapper {
    List<Map<String,Object>> findAll();
}
