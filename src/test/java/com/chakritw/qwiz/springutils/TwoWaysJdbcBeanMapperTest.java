package com.chakritw.qwiz.springutils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * TwoWaysJdbcBeanMapper Unit Test
 */
@ExtendWith(MockitoExtension.class)
public class TwoWaysJdbcBeanMapperTest {
    private static final String SCHEMA = "gg";
    private static final String ITEMS_TABLE = "test_items";
    private static final String ITEMS_JOIN_TABLE = "test_join";
    private static final String DELIM = "&&";

    // @Mock
    private DataSource mockingDataSource;
    private boolean initState;

    @PostConstruct
    public void init() {
        if (initState) {
            return;
        }
        initState = true;
    }

    private ResultSet createMockingResultSet(final Collection<Map<String, ?>> rows) throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        final Iterator<Map<String, ?>> rowsIter = rows.iterator();
        // final int n = rows.size();
        final int[] idx = new int[] { 0 };
        final List<Map<String, ?>> row = new ArrayList<>();
        row.add(null);
        // OngoingStubbing<Boolean> nextFn =
        Mockito.lenient().when(rs.next()).thenAnswer((inv) -> {
            if (!rowsIter.hasNext()) {
                return false;
            }
            idx[0]++;
            row.set(0, rowsIter.next());
            return true; // (idx[0] <= n);
        });
        // OngoingStubbing<Object> getObjFn =
        Mockito.lenient().when(rs.getObject(Mockito.anyString()))
                .thenAnswer((inv) -> row.get(0).get((String) inv.getArgument(0)));
        /*
         * for (int i = 0;i < n;i++) { getColNameFn = getColNameFn.thenReturn(cols[i]);
         * nextFn = nextFn.thenReturn(i < n-1); } nextFn = nextFn.thenReturn(false);
         */

        return rs;
    }

    @Test
    public void testColumnsMappingByRule() {
        init();
        final Pattern namePattern = Pattern.compile("^([A-Za-z_\\$]{1})([A-Za-z0-9_\\$]*)$");
        final Map<String, String> expected = new HashMap<String, String>() {{
            put("id", "Id");
            put("name", "Name");
            put("active", "IsActive");
        }};
        final TwoWaysJdbcBeanMapper<TestModel> mapper = new TwoWaysJdbcBeanMapper<>(TestModel.class);
        final List<String> propNames = new ArrayList<>();
        mapper.map((propName, propType) -> {
            if (!expected.containsKey(propName)) {
                return null;
            }
            propNames.add(propName);
            final Matcher matcher = namePattern.matcher(propName);
            if (!matcher.matches()) {
                return null;
            }
            final int nGroups = matcher.groupCount();
            final String capName = matcher.group(1).toUpperCase() + ((nGroups >= 2) ? matcher.group(2) : "");
            boolean isGetterBoolean = (propType.equals(Boolean.TYPE))
                    || (Boolean.class.isAssignableFrom(propType));
                    
            return (isGetterBoolean) ? "Is" + capName : capName;
        });

        for (String paramName : expected.values()) {
            assertTrue(mapper.hasValue(paramName));
        }
        //int i = 0;
        for (String propName : propNames) {
            assertEquals(expected.get(propName), mapper.getMappedName(propName));
            //i++;
        }
    }

    @Test
    public void testMapRow() throws SQLException {
        init();
        final List<TestModel> expected = Arrays.asList(
            new TestModel().setId(1).setName("A").setActive(true),
            new TestModel().setId(2).setName("B").setActive(true),
            new TestModel().setId(3).setName("X").setActive(false)
        );
        final List<Map<String, ?>> rows = Arrays.asList(
            new HashMap<String, Object>() {{
                put("id", 1);
                put("name", "A");
                put("is_active", true);
            }},
            new HashMap<String, Object>() {{
                put("id", 2);
                put("name", "B");
                put("is_active", true);
            }},
            new HashMap<String, Object>() {{
                put("id", 3);
                put("name", "X");
                put("is_active", false);
            }}
        );
        final ResultSet rs = createMockingResultSet(rows);
        final TwoWaysJdbcBeanMapper<TestModel> mapper = new TwoWaysJdbcBeanMapper<>(TestModel.class);
        mapper.mapSameExcept("active")
            .map("active", "is_active");
        int i = 0;
        while (rs.next()) {
            System.out.println("testMapRow(): " + rows.get(i));
            final TestModel expectedItem = expected.get(i);
            final TestModel item = mapper.mapRow(rs, i);
            assertEquals(expectedItem.getId(), item.getId());
            assertEquals(expectedItem.getName(), item.getName());
            assertEquals(expectedItem.isActive(), item.isActive());
            i++;
        }
    }

    @Test
    public void testParamsSrc() throws SQLException {
        init();
        final TestModel item = new TestModel().setId(1).setName("A").setActive(true);
        final TwoWaysJdbcBeanMapper<TestModel> mapper = new TwoWaysJdbcBeanMapper<>(item);
        mapper.mapSameExcept("active").map("active", "is_active");
        final Map<String, ?> expected = new HashMap<String, Object>() {{
            put("id", 1L);
            put("name", "A");
            put("is_active", true);
        }};
        for (Map.Entry<String, ?> entry : expected.entrySet()) {
            final String key = entry.getKey();
            final Object expectedVal = entry.getValue();
            final Object val = (mapper.hasValue(key)) ? mapper.getValue(key) : null;
            assertEquals(expectedVal, val);
        }
        System.out.println("testParamsSrc(): " + expected);
        assertTrue(true);
        /*
         * assertEquals(sql, "INSERT INTO test_items" + "(name, secret,active,remarks)"
         * + "\tVALUES(name = :name)" );
         */
    }

    protected static class TestModel {
        private long id;
        private String name;
        private String secret;
        private boolean active;
        private String remarks;

        public long getId() {
            return id;
        }

        public TestModel setId(long id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public TestModel setName(String name) {
            this.name = name;
            return this;
        }

        public String getSecret() {
            return secret;
        }

        public TestModel setSecret(String secret) {
            this.secret = secret;
            return this;
        }

        public String getRemarks() {
            return remarks;
        }

        public TestModel setRemarks(String remarks) {
            this.remarks = remarks;
            return this;
        }

        public boolean isActive() {
            return active;
        }

        public TestModel setActive(boolean active) {
            this.active = active;
            return this;
        }

    }
}
