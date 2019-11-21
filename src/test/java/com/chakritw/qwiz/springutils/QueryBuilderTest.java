package com.chakritw.qwiz.springutils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * QueryBuilder Unit Test
 */
@ExtendWith(MockitoExtension.class)
public class QueryBuilderTest {
    private static final String SCHEMA = "gg";
    private static final String ITEMS_TABLE = "test_items";
    private static final String ITEMS_JOIN_TABLE = "test_join";
    private static final String DELIM = "&&";
    
    //@Mock
    private DataSource mockingDataSource;
    private boolean initState;

    @PostConstruct
    public void init() {
        if (initState) {
            return;
        }
       // if (mockingDataSource == null) {
        try {
            /*ResultSet itemColsRS = createMockingColumnsResultSet(new String[] {
                "id", "name", "is_active", "secret", "remarks"
            });
            ResultSet itemJoinColsRS = createMockingColumnsResultSet(new String[] {
                "id", "t_id", "description"
            });
            */
            DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
            Mockito.lenient().when(metaData.getColumns(null, SCHEMA, ITEMS_TABLE, null))
                .thenAnswer((inv) -> createMockingColumnsResultSet(new String[] {
                    "id", "name", "is_active", "secret", "remarks"
                }));
            Mockito.lenient().when(metaData.getColumns(null, SCHEMA, ITEMS_JOIN_TABLE, null))
                .thenAnswer((inv) -> createMockingColumnsResultSet(new String[] {
                    "id", "t_id", "description"
                }));
            Connection con = Mockito.mock(Connection.class);
            Mockito.when(con.getMetaData()).thenReturn(metaData);
            DataSource ds = Mockito.mock(DataSource.class);
            Mockito.when(ds.getConnection()).thenReturn(con);
            mockingDataSource = ds;
        } catch (SQLException ex) {
           throw new RuntimeException(ex.getMessage(), ex);
        }
        //}
        initState = true;
    }

    private ResultSet createMockingColumnsResultSet(final String[] cols) throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        final int n = cols.length;
        final int[] idx = new int[] { 0 };
        OngoingStubbing<Boolean> nextFn = Mockito.lenient().when(rs.next())
            .thenAnswer((inv) -> {
                idx[0]++;
                return (idx[0] <= n);
            });
        OngoingStubbing<String> getColNameFn = Mockito.lenient().when(rs.getString("COLUMN_NAME"))
            .thenAnswer((inv) -> cols[idx[0] - 1]);
        /*
        for (int i = 0;i < n;i++) {
            getColNameFn = getColNameFn.thenReturn(cols[i]);
            nextFn = nextFn.thenReturn(i < n-1);
        }
        nextFn = nextFn.thenReturn(false);
        */

        return rs;
    }

    @Test
    public void testBuildSelect() throws SQLException {
        init();
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName(SCHEMA)
            .setClausesDelimiter(DELIM)
            .select((s) -> {
                s.allFromMain()
                    .allFrom("tj")
                    .except(new String[] { "t.is_active", "t.secret", "tj.id", "tj.t_id" });
                s.from(ITEMS_TABLE, "t")
                    .leftJoin(ITEMS_JOIN_TABLE, "tj", (j) -> j.on("t.id = tj.t_id"));
                s.where((w) -> {
                    w.add("is_active = :is_active")
                        .addIf((p) -> (p.getValue("name") != null), "name = :name");
                });
            });

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        params.addValue("is_active", true);
        String sql = qb.build(params);
        String expected = "SELECT t.id,t.name,t.remarks,tj.description"
            + DELIM + "FROM " + SCHEMA + "." + ITEMS_TABLE + " AS t"
            + DELIM + "LEFT JOIN " + SCHEMA + "." + ITEMS_JOIN_TABLE
            + " AS tj ON (t.id = tj.t_id)"
            + DELIM + "WHERE (is_active = :is_active)"
            + DELIM + "AND (name = :name)";
        System.out.println("testBuildSelect() => " + sql);
        assertEquals(expected, sql);
    }

    @Test
    public void testBuildPagedSelect() throws SQLException {
        init();
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setDialect(QueryBuilder.POSTGRESQL)
            .setSchemaName(SCHEMA)
            .setClausesDelimiter(DELIM)
            .select((s) -> {
                s.select((ss) -> {
                    ss.allFromMain()
                        .allFrom("tj")
                        .except(new String[] { "t.is_active", "t.secret", "tj.id", "tj.t_id" });
                    ss.from(ITEMS_TABLE, "t")
                        .leftJoin(ITEMS_JOIN_TABLE, "tj", (j) -> j.on("t.id = tj.t_id"));
                    ss.where((w) -> {
                        w.add("is_active = :is_active")
                            .addIf((p) -> (p.getValue("name") != null), "name = :name");
                    });
                    ss.orderBy(new String[] { "t.id" }, new Boolean[] { true });
                })
                .page(0, 20);
            });

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        params.addValue("is_active", true);
        String sql = qb.build(params);
        String expected = "SELECT "
            + DELIM + "(SELECT t.id,t.name,t.remarks,tj.description"
            + DELIM + "FROM " + SCHEMA + "." + ITEMS_TABLE + " AS t"
            + DELIM + "LEFT JOIN " + SCHEMA + "." + ITEMS_JOIN_TABLE
            + " AS tj ON (t.id = tj.t_id)"
            + DELIM + "WHERE (is_active = :is_active)"
            + DELIM + "AND (name = :name)"
            + DELIM + "ORDER BY t.id DESC)"
            + DELIM + "OFFSET 0 LIMIT 20";
        System.out.println("testBuildPagedSelect() => " + sql);
        assertEquals(expected, sql);
    }

    @Test
    public void testBuildInsertVals() throws SQLException {
        init();
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName(SCHEMA)
            .setClausesDelimiter(DELIM)
            .insert(ITEMS_TABLE, "id", (i) -> {
            });
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        params.addValue("is_active", true);
        String sql = qb.build(params);
        // TODO:
        System.out.println("testBuildInsertVals() => " + sql);
        assertTrue(true);
        /*
         * assertEquals(sql, "INSERT INTO test_items" + "(name, secret,active,remarks)"
         * + "\tVALUES(name = :name)" );
         */
    }

    @Test
    public void testBuildInsertSelect() throws SQLException {
        init();
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName(SCHEMA)
            .setClausesDelimiter(DELIM)
            .insert("test_items", "id", (i) -> {
                i.fromSelect((is) -> {
                    is.except("id").from(ITEMS_TABLE).where("name = :name");
                });
            });

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        String sql = qb.build(params);
        // TODO:
        System.out.println("testBuildInsertSelect() => " + sql);
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

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }

        public String getRemarks() {
            return remarks;
        }

        public void setRemarks(String remarks) {
            this.remarks = remarks;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

    }
}
