package com.chakritw.qwiz.springutils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

public class JsonPropertySourceFactory implements PropertySourceFactory {
    protected final ObjectMapper objectMapper;

    public JsonPropertySourceFactory() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public PropertySource<?> createPropertySource(String name, EncodedResource res) throws IOException {
        Map m = objectMapper.readValue(res.getReader(), Map.class);
        
        return new MapPropertySource("json-property", m);
    }

}