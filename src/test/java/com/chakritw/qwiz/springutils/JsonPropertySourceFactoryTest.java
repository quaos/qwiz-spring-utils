package com.chakritw.qwiz.springutils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * JsonPropertySourceFactory Integration Test
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = DummyApp.class)
@ComponentScan("com.chakritw.qwiz.springutils")
public class JsonPropertySourceFactoryTest 
{
    @Autowired
    JsonAppConfig config;

    @Test
    public void testReadJson()
    {
        JsonAppConfig expected = new JsonAppConfig() {{
            setName("qwiz-spring-utils-test");
            setData(new HashMap<String, Object>() {{
                put("a", 1);
                put("b", 2);
                put("c", "333");
            }});
        }};
        assertEquals(config.getName(), expected.getName());
        Map<String, Object> data = config.getData();
        assertNotNull(data);
        assertTrue(data.entrySet().stream().allMatch((e) -> e.getValue().equals(expected.getData().get(e.getKey()))));
    }
}
