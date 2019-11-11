package com.chakritw.qwiz.springutils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * JsonPropertySourceFactory Integration Test
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = App.class)
public class JsonPropertySourceFactoryTest 
{
    @Autowired
    ApplicationConfig config;

    @Test
    public void testReadJson()
    {
        ApplicationConfig expected = new ApplicationConfig() {{
            setName("qwiz-spirng-utils-test");
            setData(new HashMap<String, Object>() {{
                put("a", 1);
                put("b", 1);
                put("c", "333");
            }});
        }};
        assertEquals(config.getName(), expected.getName());
        Map<String, Object> data = config.getData();
        assertNotNull(data);
        assertTrue(data.entrySet().stream().allMatch((e) -> e.getValue().equals(expected.getData().get(e.getKey()))));
    }

    @ConfigurationProperties
    @PropertySource(value = "classpath:app-test.json",
        factory = JsonPropertySourceFactory.class)
    public static class ApplicationConfig {
        private String name;
        private Map<String, Object> data;

        public ApplicationConfig() {
        }

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }

        public Map<String, Object> getData() {
            return data;
        }
        public void setData(Map<String, Object> data) {
            this.data = data;
        }
        
    }
}
