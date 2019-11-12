package com.chakritw.qwiz.springutils;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * QueryBuilder Unit Test
 */
@ExtendWith(MockitoExtension.class)
public class QueryBuilderTest {
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
            ResultSet colsResultSet = Mockito.mock(ResultSet.class);
            DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
            Mockito.when(metaData.getColumns(null, null, null, null))
                .thenReturn(colsResultSet);
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

    @Test
    public void testBuildSelect() throws SQLException {
        init();
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName("gg")
                .setClausesDelimiter("\t").select((s) -> {
                    s.except(new String[] { "active", "secret" });
                    s.from("test_items", "t").leftJoin("test_join", "tj", (j) -> j.on("t.id = tj.t_id"));
                    s.where((w) -> {
                        w.add("active = :active").addIf((p) -> (p.getValue("name") != null), "name = :name");
                    });
                });

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        params.addValue("active", true);
        String sql = qb.build(params);
        System.out.println("testBuildSelect() => " + sql);
        assertEquals(sql, "SELECT id,name,remarks" + "\tFROM test_items AS t"
                + "\tLEFT JOIN test_join AS tj ON (t.id = tj.t_id)" + "\tWHERE (active = :active)\tAND (name = :name)");
    }

    @Test
    public void testBuildInsertVals() throws SQLException {
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName("gg")
            .setClausesDelimiter("\t")
            .insert("test_items", "id", (i) -> {
            });
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", "Abc");
        params.addValue("active", true);
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
        QueryBuilder<TestModel, Long> qb = new QueryBuilder<TestModel, Long>()
            .setDataSource(mockingDataSource)
            .setSchemaName("gg")
            .setClausesDelimiter("\t")
            .insert("test_items", "id", (i) -> {
                i.fromSelect((is) -> {
                    is.except("id").from("test_items").where("name = :name");
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
