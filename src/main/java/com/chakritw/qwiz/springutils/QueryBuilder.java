package com.chakritw.qwiz.springutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * TODO: Copy to qwiz-spring-utils
 * 
 */
public class QueryBuilder<T, TKey> {
    protected String tableName;
    protected DataSource dataSource;
    protected NamedParameterJdbcTemplate jdbcTemplate;
    protected SqlParameterSource parameters;

    protected OpClause opClause;
    //protected WhereClause whereClause;

    public QueryBuilder() {
    }

    public QueryBuilder select() {
        select(null, null);
        return this;
    }
    public QueryBuilder select(String[] conds) {
        select(conds, null);
        return this;
    }
    public QueryBuilder select(Consumer<SelectClause> processFn) {
        select(null, processFn);
        return this;
    }
    public QueryBuilder select(String[] columns, Consumer<SelectClause> processFn) {
        SelectClause clause = new SelectClause(Arrays.asList(columns));
        if (processFn != null) {
            processFn.accept(clause);
        }
        this.opClause = clause;
        return this;
    }


    public QueryBuilder insert(Consumer<OpClause> processFn) {
        //TODO:
        SelectClause clause = new SelectClause();
        if (processFn != null) {
            processFn.accept(clause);
        }
        this.opClause = clause;
        return this;
    }

    public QueryBuilder update(Consumer<OpClause> processFn) {
        //TODO:
        SelectClause clause = new SelectClause();
        if (processFn != null) {
            processFn.accept(clause);
        }
        this.opClause = clause;
        return this;
    }
    
    public QueryBuilder delete(Consumer<OpClause> processFn) {
        //TODO:
        SelectClause clause = new SelectClause();
        if (processFn != null) {
            processFn.accept(clause);
        }
        this.opClause = clause;
        return this;
    }
    
    public List<T> query(SqlParameterSource params, TwoWaysJdbcBeanMapper<T> mapper) {
        return query(mapper, mapper);
    }
    public List<T> query(SqlParameterSource params, RowMapper<T> rowMapper) throws DataAccessException {
        
        String sql;
        try {
            sql = build(params);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException(ex.getMessage(), ex);
        }

        return jdbcTemplate.query(sql, params, rowMapper);
    }
    public int exec(SqlParameterSource params) throws DataAccessException {
        String sql;
        try {
            sql = build(params);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException(ex.getMessage(), ex);
        }

        return jdbcTemplate.update(sql, params);
    }
    public TKey execInsert(T item, Consumer<TKey> setKeyFn) throws DataAccessException {
        TwoWaysJdbcBeanMapper beanMapper = new TwoWaysJdbcBeanMapper<T>(item);
        TKey key = execInsert(beanMapper, (Class<T>)item.getClass());
        if ((key != null) && (setKeyFn != null)) {
            setKeyFn.accept(key);
        }
        return key;
    }
    public TKey execInsert(SqlParameterSource params, Class<T> resultType) throws DataAccessException {
        String sql;
        try {
            sql = build(params);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException(ex.getMessage(), ex);
        }
        //final ObjectMapper objectMapper = new ObjectMapper();
        
        return jdbcTemplate.execute(sql, params, (result) -> {
            ResultSet keysRS = result.getGeneratedKeys();
            //TODO:
            Object key = keysRS.getObject(0);
            return (TKey)key;
            //return objectMapper.convertValue(fromValue, toValueType);
        });
    }

    public String build(SqlParameterSource params) throws SQLException {
        StringBuilder sqlb = new StringBuilder();
        DatabaseMetaData metadata = null;
        try (Connection conn = dataSource.getConnection()) {
            metadata = conn.getMetaData();
        }
        sqlb.append(opClause.build(params, metadata));

        return sqlb.toString();
    }


    protected static abstract class QueryPart {
        public abstract String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException;
    }

    protected static class StaticQueryClause extends QueryPart {
        protected final String sql;

        public StaticQueryClause(String sql) {
            this.sql = sql;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) {
            return sql;
        }
    }
    
    protected static class IfExpr extends QueryPart {
        protected final Function<SqlParameterSource, Boolean> condFn;
        protected final QueryPart onTrueClause;
        protected final QueryPart onFalseClause;

        public IfExpr(Function<SqlParameterSource, Boolean> condFn, final QueryPart onTrueClause) {
            this(condFn, onTrueClause, null);
        }
        public IfExpr(Function<SqlParameterSource, Boolean> condFn,
            final QueryPart onTrueClause, final QueryPart onFalseClause)
        {
            super();
            this.condFn = condFn;
            this.onTrueClause = onTrueClause;
            this.onFalseClause = onFalseClause;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            Boolean condResult = condFn.apply(params);
            QueryPart resultClause = ((condResult != null) && (condResult.booleanValue()))
                ? onTrueClause
                : onFalseClause;

            return (resultClause != null) ? resultClause.build(params, metadata) : "";
        }
    }
    
    
    protected static abstract class OpClause extends QueryPart {
        protected DatabaseMetaData metaData;

        public OpClause setMetaData(DatabaseMetaData metaData) {
            this.metaData = metaData;
            return this;
        }
    }

    protected static class SelectClause extends OpClause {
        protected final List<String> excludedCols;
        protected final List<QueryPart> subClauses;
        protected FromClause fromClause;
        protected final List<JoinClause> joinClauses;
        protected WhereClause whereClause;

        protected String asName;

        public SelectClause() {
            this.subClauses = new ArrayList<>();
            this.joinClauses = new ArrayList<>();
            this.excludedCols = new ArrayList<>();
        }
        public SelectClause(List<String> cols) {
            this();
            add(cols);
        }

        public void setAsName(String asName) {
            this.asName = asName;
        }

        public SelectClause add(List<String> cols) {
            cols.forEach((col) -> add(col));
            return this;
        }
        public SelectClause add(String col) {
            subClauses.add(new StaticQueryClause(col));
            return this;
        }
        public SelectClause add(String col, String asName) {
            subClauses.add(new StaticQueryClause(col + " AS " + asName));
            return this;
        }

        public SelectClause except(String col) {
            excludedCols.add(col);
            return this;
        } 

        public SelectClause select(Consumer<SelectClause> processFn) {
            //Subquery select
            return this.select(null, processFn);
        }
        public SelectClause select(String[] cols) {
            //Subquery select
            return this.select(cols, null);
        }
        public SelectClause select(String[] cols, Consumer<SelectClause> processFn) {
            //Subquery select
            SelectClause subQuery = new SelectClause((cols != null) ? Arrays.asList(cols) : null);
            if (processFn != null) {
                processFn.accept(subQuery);
            }
            subClauses.add(subQuery);
            return this;
        }

        public SelectClause where() {
            setWhereClause(new WhereClause(), null);
            return this;
        }
        public SelectClause where(String[] conds) {
            where(conds, null);
            setWhereClause(new WhereClause(Arrays.asList(conds)), null);
            return this;
        }
        public SelectClause where(Consumer<WhereClause> processFn) {
            setWhereClause(new WhereClause(), processFn);
            return this;
        }
        public SelectClause where(String[] conds, Consumer<WhereClause> processFn) {
            setWhereClause(new WhereClause(Arrays.asList(conds)), processFn);
            return this;
        }
        protected void setWhereClause(WhereClause clause, Consumer<WhereClause> processFn) {
            if (processFn != null) {
                processFn.accept(clause);
            }
            this.whereClause = clause;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("SELECT ");
            final int n = subClauses.size();
            if (n <= 0) {
                ResultSet colsRS = metadata.getColumns(null, null, fromClause.tableName, null);
            }
            for (int i=0;i < n;i++) {
                if (i > 0) {
                    sqlb.append(',');
                }
                QueryPart clause = subClauses.get(i);
                if (clause == null) {
                    continue;
                }
                if (clause instanceof SelectClause) {
                    sqlb.append('(');
                    sqlb.append(clause.build(params, metadata));
                    sqlb.append(')');
                } else {
                    sqlb.append(clause.build(params, metadata));
                }
            }
            sqlb.append(fromClause.build(params, metadata));
            for (QueryPart clause : joinClauses) {
                sqlb.append(clause.build(params, metadata));
            }
            if (whereClause != null) {
                sqlb.append(whereClause.build(params, metadata));
            }

            return sqlb.toString();
        }

    }
    
    protected static class FromClause extends QueryPart {
        protected String tableName;
        protected String asName;

        public FromClause(String table) {
            super();
            this.tableName = table;
        }
        public FromClause(String table, String asName) {
            this(table);
            this.asName = asName;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("FROM ");
            sqlb.append(tableName);
            if ((this.asName != null) && (!asName.isEmpty())) {
                sqlb.append(" AS ");
                sqlb.append(this.asName);
            }
            return sqlb.toString();
        }
    }
    
    protected static class JoinClause extends FromClause {
        protected final String type;
        protected final List<QueryPart> onClauses;

        public JoinClause(final String type, final String table) {
            this(type, table, null);
        }
        public JoinClause(final String type, final String table, final  String asName) {
            super(table, asName);
            this.type = type;
            this.asName = asName;
            this.onClauses = new ArrayList<>();
        }

        public JoinClause on(String cond) {
            onClauses.add(new StaticQueryClause(cond));
            return this;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append(type);
            sqlb.append("JOIN ");
            sqlb.append(tableName);
            if ((this.asName != null) && (!asName.isEmpty())) {
                sqlb.append(" AS ");
                sqlb.append(this.asName);
            }
            return sqlb.toString();
        }
    }


    protected static class WhereClause extends QueryPart {
        protected final List<QueryPart> subClauses;
        protected DatabaseMetaData metaData;
        protected String conj = "AND";

        public WhereClause setMetaData(DatabaseMetaData metaData) {
            this.metaData = metaData;
            return this;
        }

        public WhereClause setConj(String conj) {
            this.conj = conj;
            return this;
        }

        public WhereClause() {
            this.subClauses = new ArrayList<>();
        }
        public WhereClause(List<String> conds) {
            this();
            add(conds);
        }

        public WhereClause and(List<String> conds) {
            setConj("AND");
            add(conds);
            return this;
        }
        public WhereClause and(String cond) {
            setConj("AND");
            add(cond);
            return this;
        }

        public WhereClause or(List<String> conds) {
            setConj("OR");
            add(conds);
            return this;
        }
        public WhereClause or(String cond) {
            setConj("OR");
            add(cond);
            return this;
        }

        public WhereClause add(List<String> conds) {
            conds.forEach((cond) -> add(cond));
            return this;
        }
        public WhereClause add(String cond) {
            subClauses.add(new StaticQueryClause(cond));
            return this;
        }
        public WhereClause addIf(String cond, boolean active) {
            if (!active) {
                return this;
            }
            subClauses.add(new StaticQueryClause(cond));
            return this;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("WHERE ");
            final int n = subClauses.size();
            for (int i=0;i < n;i++) {
                if (i > 0) {
                    sqlb.append(',');
                }
                QueryPart clause = subClauses.get(i);
                if (clause == null) {
                    continue;
                }
                if (clause instanceof WhereClause) {
                    sqlb.append('(');
                    sqlb.append(clause.build(params, metadata));
                    sqlb.append(')');
                } else {
                    sqlb.append(clause.build(params, metadata));
                }
            }

            return sqlb.toString();
        }
    }
}