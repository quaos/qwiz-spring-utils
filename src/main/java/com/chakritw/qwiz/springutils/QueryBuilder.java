package com.chakritw.qwiz.springutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * TODO: Copy to qwiz-spring-utils
 * 
 */
public class QueryBuilder<T, TKey> {
    protected String schemaName;
    //protected String tableName;
    protected String clausesDelimiter = "\n";
    protected DataSource dataSource;
    protected NamedParameterJdbcTemplate jdbcTemplate;
    protected SqlParameterSource parameters;

    protected OpClause opClause;
    //protected WhereClause whereClause;

    public QueryBuilder() {
    }

    public QueryBuilder<T, TKey> setSchemaName(String schema) {
        this.schemaName = schema;
        return this;
    }

    public QueryBuilder<T, TKey> setClausesDelimiter(String delim) {
        this.clausesDelimiter = delim;
        return this;
    }

    public QueryBuilder<T, TKey> setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbcTemplate = (dataSource != null)
            ? new NamedParameterJdbcTemplate(dataSource)
            : null;
        return this;
    }

    public QueryBuilder<T, TKey> select() {
        setOpClause(this.new SelectClause(), null);
        return this;
    }
    public QueryBuilder<T, TKey> select(String[] columns) {
        setOpClause(this.new SelectClause(Arrays.asList(columns)), null);
        return this;
    }
    public QueryBuilder<T, TKey> select(Consumer<SelectClause> processFn) {
        setOpClause(this.new SelectClause(), processFn);
        return this;
    }
    public QueryBuilder<T, TKey> select(String[] columns, Consumer<SelectClause> processFn) {
        setOpClause(this.new SelectClause(Arrays.asList(columns)), processFn);
        return this;
    }
    public QueryBuilder<T, TKey> insert(String table, Consumer<InsertClause> processFn) {
        setOpClause(this.new InsertClause(table), processFn);
        return this;
    }
    public QueryBuilder<T, TKey> insert(String table, String idCol, Consumer<InsertClause> processFn) {
        setOpClause(this.new InsertClause(table, idCol), processFn);
        return this;
    }

    public QueryBuilder<T, TKey> update(String table, Consumer<UpdateClause> processFn) {
        setOpClause(this.new UpdateClause(table), processFn);
        return this;
    }
    public QueryBuilder<T, TKey> update(String table, String idCol, Consumer<UpdateClause> processFn) {
        setOpClause(this.new UpdateClause(table, idCol), processFn);
        return this;
    }
    
    public QueryBuilder<T, TKey> delete(String table, Consumer<DeleteClause> processFn) {
        setOpClause(this.new DeleteClause(table), processFn);
        return this;
    }
    public QueryBuilder<T, TKey> delete(String table, String idCol, Consumer<DeleteClause> processFn) {
        setOpClause(this.new DeleteClause(table, idCol), processFn);
        return this;
    }

    protected void setOpClause(OpClause clause, Consumer<? extends OpClause> processFn) {
        if (processFn != null) {
            ((Consumer<OpClause>)processFn).accept(clause);
        }
        this.opClause = clause;
    }

    public List<T> execQuery(SqlParameterSource params, TwoWaysJdbcBeanMapper<T> mapper) {
        return execQuery(mapper, mapper);
    }
    public List<T> execQuery(SqlParameterSource params, RowMapper<T> rowMapper) throws DataAccessException {
        
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
    public TKey execInsert(T item, Consumer<TKey> keySetter) throws DataAccessException {
        TwoWaysJdbcBeanMapper beanMapper = new TwoWaysJdbcBeanMapper<T>(item);
        TKey key = execInsert(beanMapper, (Class<T>)item.getClass());
        if ((key != null) && (keySetter != null)) {
            keySetter.accept(key);
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

    protected void appendFullTableName(final String tableName, final StringBuilder sb) {
        if ((schemaName != null) && (!schemaName.isEmpty())) {
            sb.append(schemaName);
            sb.append('.');
        }
        sb.append(tableName);
    }

    protected List<String> getIncludedColumns(final String tableName,
        final List<String> columns, final List<String> excludedColumns,
        final DatabaseMetaData metadata) throws SQLException
    {
        final List<String> cols2 = ((columns == null) || (columns.isEmpty()))
            ? getColumnsFromMetadata(tableName, metadata)
            : columns;
        if (excludedColumns != null) {
            cols2.removeAll(excludedColumns);
        }

        return cols2;
    }
    protected List<String> getColumnsFromMetadata(final String tableName, final DatabaseMetaData metadata)
            throws SQLException
    {
        final List<String> cols = new ArrayList<>();
        final ResultSet colsRS = metadata.getColumns(null, this.schemaName, tableName, null);
        while (colsRS.next()) {
            String name = colsRS.getString("COLUMN_NAME");
            //String type = colsRS.getString("TYPE_NAME");
            //int size = colsRS.getInt("COLUMN_SIZE");
            cols.add(name);
        }

        return cols;
    }

    public static abstract class QueryPart {
        protected final QueryPart parent;

        protected QueryPart() {
            this(null);
        }
        protected QueryPart(final QueryPart parent) {
            this.parent = parent;
        }
            
        public abstract String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException;
    }

    public static class StaticQueryClause extends QueryPart {
        protected final String sql;

        public StaticQueryClause(final QueryPart parent, final String sql) {
            super(parent);
            this.sql = sql;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) {
            return sql;
        }
    }
    

    public static class IfExpr extends QueryPart {
        protected final Function<SqlParameterSource, Boolean> condFn;
        protected final QueryPart onTrueClause;
        protected final QueryPart onFalseClause;

        public IfExpr(final QueryPart parent, Function<SqlParameterSource, Boolean> condFn, final QueryPart onTrueClause) {
            this(parent, condFn, onTrueClause, null);
        }
        public IfExpr(final QueryPart parent, Function<SqlParameterSource, Boolean> condFn,
            final QueryPart onTrueClause, final QueryPart onFalseClause)
        {
            super(parent);
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
    
    public static abstract class OpClause extends QueryPart {
        public OpClause() {
        }
        public OpClause(QueryPart parent) {
            super(parent);
        }
    }

    public class SelectClause extends OpClause {
        protected final Map<String, String> tableNamesByAliasMap;
        protected boolean allFromMain;
        protected final List<String> allFromTables;
        protected final List<String> excludedCols;
        protected final List<QueryPart> subClauses;
        protected FromClause fromClause;
        protected final List<JoinClause> joinClauses;
        protected WhereClause whereClause;
        protected final List<QueryPart> afterWhereClauses;

        protected String asName;

        public SelectClause() {
            this((QueryPart)null);
        }
        public SelectClause(final QueryPart parent) {
            super(parent);
            this.tableNamesByAliasMap = new HashMap<>();
            this.allFromTables = new ArrayList<>();
            this.excludedCols = new ArrayList<>();
            this.subClauses = new ArrayList<>();
            this.joinClauses = new ArrayList<>();
            this.afterWhereClauses = new ArrayList<>();
        }
        public SelectClause(List<String> cols) {
            this((QueryPart)null);
            add(cols);
        }
        public SelectClause(final QueryPart parent, List<String> cols) {
            this(parent);
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
            subClauses.add(new StaticQueryClause(this, col));
            return this;
        }
        public SelectClause add(String col, String asName) {
            subClauses.add(new StaticQueryClause(this, col + " AS " + asName));
            return this;
        }

        public SelectClause allFromMain() {
            this.allFromMain = true;
            return this;
        }
        public SelectClause allFrom(String tbl) {
            allFromTables.add(tbl);
            return this;
        }
        public SelectClause except(String[] cols) {
            except(Arrays.asList(cols));
            return this;
        }
        public SelectClause except(List<String> cols) {
            excludedCols.addAll(cols);
            return this;
        } 
        public SelectClause except(String col) {
            excludedCols.add(col);
            return this;
        } 

        public SelectClause select(Consumer<SelectClause> processFn) {
            addSubSelectClause(new SelectClause(), processFn);
            return this;
        }
        public SelectClause select(String[] cols) {
            //Subquery select
            addSubSelectClause(new SelectClause(Arrays.asList(cols)), null);
            return this;
        }
        public SelectClause select(String[] cols, Consumer<SelectClause> processFn) {
            addSubSelectClause(new SelectClause(Arrays.asList(cols)), processFn);
            return this;
        }
        protected void addSubSelectClause(final SelectClause subClause, final Consumer<SelectClause> processFn) {
            //Subquery select
            if (processFn != null) {
                processFn.accept(subClause);
            }
            subClauses.add(subClause);
        }

        public SelectClause from(String table) {
            setFromClause(new FromClause(table), null);
            return this;
        }
        public SelectClause from(String table, Consumer<FromClause> processFn) {
            setFromClause(new FromClause(table), processFn);
            return this;
        }
        public SelectClause from(String table, String asName) {
            setFromClause(new FromClause(table, asName), null);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        public SelectClause from(String table, String asName, Consumer<FromClause> processFn) {
            setFromClause(new FromClause(table, asName), processFn);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        protected void setFromClause(final FromClause clause, final Consumer<? extends FromClause> processFn) {
            if (processFn != null) {
                ((Consumer<FromClause>)processFn).accept(clause);
            }
            this.fromClause = clause;
        }

        public SelectClause innerJoin(String table, String onClause) {
            addJoinClause(new JoinClause(JoinClause.INNER, table).on(onClause), null);
            return this;
        }
        public SelectClause innerJoin(String table, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.INNER, table), processFn);
            return this;
        }
        public SelectClause innerJoin(String table, String asName, String onClause) {
            addJoinClause(new JoinClause(JoinClause.INNER, table, asName).on(onClause), null);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        public SelectClause innerJoin(String table, String asName, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.INNER, table, asName), processFn);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }

        public SelectClause leftJoin(String table, String onClause) {
            addJoinClause(new JoinClause(JoinClause.LEFT, table).on(onClause), null);
            return this;
        }
        public SelectClause leftJoin(String table, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.LEFT, table), processFn);
            return this;
        }
        public SelectClause leftJoin(String table, String asName, String onClause) {
            addJoinClause(new JoinClause(JoinClause.LEFT, table, asName).on(onClause), null);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        public SelectClause leftJoin(String table, String asName, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.LEFT, table, asName), processFn);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        
        public SelectClause rightJoin(String table, String onClause) {
            addJoinClause(new JoinClause(JoinClause.RIGHT, table).on(onClause), null);
            return this;
        }
        public SelectClause rightJoin(String table, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.RIGHT, table), processFn);
            return this;
        }
        public SelectClause rightJoin(String table, String asName, String onClause) {
            addJoinClause(new JoinClause(JoinClause.RIGHT, table, asName).on(onClause), null);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }
        public SelectClause rightJoin(String table, String asName, Consumer<JoinClause> processFn) {
            addJoinClause(new JoinClause(JoinClause.RIGHT, table, asName), processFn);
            tableNamesByAliasMap.put(asName, table);
            return this;
        }

        protected void addJoinClause(final JoinClause clause,
            final Consumer<? extends JoinClause> processFn)
        {
            if (processFn != null) {
                ((Consumer<JoinClause>)processFn).accept(clause);
            }
            this.joinClauses.add(clause);
        }


        public SelectClause where() {
            setWhereClause(new WhereClause(this), null);
            return this;
        }
        public SelectClause where(String cond) {
            setWhereClause(new WhereClause(this).add(cond), null);
            return this;
        }
        public SelectClause where(String[] conds) {
            setWhereClause(new WhereClause(this, Arrays.asList(conds)), null);
            return this;
        }
        public SelectClause where(Consumer<WhereClause> processFn) {
            setWhereClause(new WhereClause(this), processFn);
            return this;
        }
        public SelectClause where(String[] conds, Consumer<WhereClause> processFn) {
            setWhereClause(new WhereClause(this, Arrays.asList(conds)), processFn);
            return this;
        }
        protected void setWhereClause(final WhereClause clause, final Consumer<WhereClause> processFn) {
            if (processFn != null) {
                processFn.accept(clause);
            }
            this.whereClause = clause;
        }

        public SelectClause orderBy(String[] cols) {
            addAfterWhereClause(new OrderByClause(Arrays.asList(cols)), null);
            return this;
        }
        public SelectClause orderBy(String[] cols, Consumer<OrderByClause> processFn) {
            addAfterWhereClause(new OrderByClause(Arrays.asList(cols)), processFn);
            return this;
        }
        public SelectClause orderBy(String[] cols, Boolean[] descs) {
            addAfterWhereClause(new OrderByClause(Arrays.asList(cols), Arrays.asList(descs)), null);
            return this;
        }
        public SelectClause orderBy(String[] cols, Boolean[] descs, Consumer<OrderByClause> processFn) {
            addAfterWhereClause(new OrderByClause(Arrays.asList(cols), Arrays.asList(descs)), processFn);
            return this;
        }
        protected void addAfterWhereClause(final AfterWhereClause clause,
            final Consumer<? extends AfterWhereClause> processFn)
        {
            if (processFn != null) {
                ((Consumer<AfterWhereClause>)processFn).accept(clause);
            }
            this.afterWhereClauses.add(clause);
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("SELECT ");
            final String tableName = fromClause.tableName;
            final List<QueryPart> subClauses2 = new ArrayList<>();
            final List<String> allFromTables2 = new ArrayList<>();
            if (allFromMain) {
                allFromTables2.add((fromClause.asName != null) ? fromClause.asName : fromClause.tableName);
            }
            allFromTables2.addAll(allFromTables);
            for (String tbl : allFromTables2) {
                String tblAlias = null;
                String fullTblName;
                if (tableNamesByAliasMap.containsKey(tbl)) {
                    tblAlias = tbl;
                    fullTblName = tableNamesByAliasMap.get(tbl);
                } else {
                    fullTblName = tbl;
                    for (Map.Entry<String, String> e : tableNamesByAliasMap.entrySet()) {
                        if (e.getValue().equals(tbl)) {
                            tblAlias = e.getKey();
                            break;
                        }
                    }
                }
                final List<String> tblCols = getColumnsFromMetadata(fullTblName, metadata);
                for (String col : tblCols) {
                    if ((excludedCols.contains(col))
                        || ((tblAlias != null) && (excludedCols.contains(tblAlias+"."+col)))
                        || (excludedCols.contains(fullTblName+"."+col)))
                    {
                        continue;
                    }
                    subClauses2.add(new StaticQueryClause(this, tbl + "." + col));
                }
            }
            final int n = subClauses.size();
            if (n > 0) {
                subClauses2.addAll(subClauses);
            }
            int n2 = subClauses2.size();
            if (n2 <= 0) {
                final List<String> cols = getIncludedColumns(tableName, null, excludedCols, metadata);
                cols.forEach((col) -> subClauses2.add(new StaticQueryClause(this, col)));
                n2 = subClauses2.size();
            }
            for (int i=0;i < n2;i++) {
                if (i > 0) {
                    sqlb.append(',');
                }
                QueryPart clause = subClauses2.get(i);
                if (clause == null) {
                    continue;
                }
                if (clause instanceof QueryBuilder.SelectClause) {
                    sqlb.append(clausesDelimiter);
                    sqlb.append('(');
                    sqlb.append(clause.build(params, metadata));
                    sqlb.append(')');
                } else {
                    sqlb.append(clause.build(params, metadata));
                }
            }
            sqlb.append(clausesDelimiter);
            sqlb.append(fromClause.build(params, metadata));
            if (!joinClauses.isEmpty()) {
                for (QueryPart clause : joinClauses) {
                    sqlb.append(clausesDelimiter);
                    sqlb.append(clause.build(params, metadata));
                }
            }
            if (whereClause != null) {
                sqlb.append(clausesDelimiter);
                sqlb.append(whereClause.build(params, metadata));
            }
            if (!afterWhereClauses.isEmpty()) {
                sqlb.append(clausesDelimiter);
                for (QueryPart clause : afterWhereClauses) {
                    sqlb.append(clause.build(params, metadata));
                }
            }
            
            return sqlb.toString();
        }

    }
    
    public class UpdateClause extends OpClause {
        protected final String tableName;
        protected final List<String> columns;
        protected final List<String> excludedCols;
        protected String idCol;
        protected WhereClause whereClause;

        public UpdateClause(final String tableName) {
            super();
            this.tableName = tableName;
            this.columns = new ArrayList<>();
            this.excludedCols = new ArrayList<>();
        }
        public UpdateClause(final String tableName, final List<String> columns) {
            this(tableName);
            this.columns.addAll(columns);
        }
        public UpdateClause(final String tableName, final String idCol) {
            this(tableName);
            withId(idCol);
        }

        public UpdateClause setColumns(List<String> cols) {
            columns.clear();
            columns.addAll(cols);
            return this;
        }
        public UpdateClause withId(String idCol) {
            excludedCols.clear();
            excludedCols.add(idCol);
            whereIdMatches(idCol);
            return this;
        }
        public UpdateClause except(String[] cols) {
            except(Arrays.asList(cols));
            return this;
        }
        public UpdateClause except(List<String> cols) {
            excludedCols.addAll(cols);
            return this;
        }
        public UpdateClause except(String col) {
            excludedCols.add(col);
            return this;
        }

        public void where(String cond) {
            addWhereClause(new WhereClause().add(cond), null);
        }
        public void where(String cond, Consumer<WhereClause> processFn) {
            addWhereClause(new WhereClause().add(cond), processFn);
        }
        public void where(QueryPart clause, Consumer<WhereClause> processFn) {
            addWhereClause(new WhereClause().add(clause), processFn);
        }
        public void whereIdMatches(String idCol) {
            addWhereClause(new WhereClause().add(new StaticQueryClause(this, idCol + " = :" + idCol)), null);
        }
        public void whereIdMatches(String idCol, Consumer<WhereClause> processFn) {
            addWhereClause(new WhereClause().add(new StaticQueryClause(this, idCol + " = :" + idCol)), processFn);
        }
        protected void addWhereClause(final WhereClause clause, final Consumer<? extends WhereClause> processFn) {
            if (whereClause != null) {
                whereClause.add(clause);
            } else {
                this.whereClause = clause;
            }
            whereClause.add(clause);
            if (processFn != null) {
                ((Consumer<WhereClause>)processFn).accept(whereClause);
            }
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            final StringBuilder sqlb = new StringBuilder();
            sqlb.append("UPDATE ");
            appendFullTableName(tableName, sqlb);
            sqlb.append("SET ");
            final List<String> cols = getIncludedColumns(tableName, columns, excludedCols, metadata);
            final int nCols = cols.size();
            for (int i=0;i < nCols;i++) {
                if (i > 0) {
                    sqlb.append(",");
                }
                final String col = cols.get(i);
                sqlb.append(col);
                sqlb.append("=:");
                sqlb.append(col);
            }
            if (this.whereClause != null) {
                sqlb.append(clausesDelimiter);
                sqlb.append(whereClause.build(params, metadata));
            }

            return sqlb.toString();
        }
        
    }

    public class InsertClause extends UpdateClause {
        protected SelectClause selectClause;

        public InsertClause(final String tableName) {
            super(tableName);
        }
        public InsertClause(final String tableName, final String idColName) {
            this(tableName);
            super.withId(idColName);
        }

        @Override
        public InsertClause setColumns(List<String> cols) {
            super.setColumns(cols);
            return this;
        }

        @Override
        public InsertClause withId(String idCol) {
            super.withId(idCol);
            return this;
        }

        @Override
        public InsertClause except(String[] cols) {
            super.except(cols);
            return this;
        }
        public InsertClause except(List<String> cols) {
            super.except(cols);
            return this;
        }
        @Override
        public InsertClause except(String col) {
            super.except(col);
            return this;
        }

        public InsertClause fromSelect(Consumer<? extends SelectClause> processFn) {
            setSelectClause(new SelectClause(), processFn);
            return this;
        }
        public InsertClause fromSelect(String[] cols, Consumer<? extends SelectClause> processFn) {
            setSelectClause(new SelectClause(Arrays.asList(cols)), processFn);
            return this;
        }
        public InsertClause fromSelect(List<String> cols, Consumer<? extends SelectClause> processFn) {
            setSelectClause(new SelectClause(cols), processFn);
            return this;
        }
        public InsertClause fromSelect(SelectClause clause) {
            setSelectClause(clause, null);
            return this;
        }
        protected void setSelectClause(SelectClause clause, Consumer<? extends SelectClause> processFn) {
            if (processFn != null) {
                ((Consumer<SelectClause>)processFn).accept(clause);
            }
            this.selectClause = clause;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            
            final StringBuilder sqlb = new StringBuilder();
            final StringBuilder valsSB = new StringBuilder();
            sqlb.append("INSERT INTO ");
            appendFullTableName(tableName, sqlb);
            sqlb.append(clausesDelimiter);
            sqlb.append('(');
            final List<String> cols = getIncludedColumns(tableName, columns, excludedCols, metadata);
            final int nCols = cols.size();
            for (int i=0;i < nCols;i++) {
                if (i > 0) {
                    sqlb.append(',');
                    if (selectClause == null) {
                        valsSB.append(',');
                    }
                }
                final String col = cols.get(i);
                sqlb.append(col);
                if (selectClause == null) {
                    valsSB.append(':');
                    valsSB.append(col);
                }
            }
            sqlb.append(')');
            sqlb.append(clausesDelimiter);
            if (selectClause != null) {
                sqlb.append(selectClause.build(params, metadata));
            } else {
                sqlb.append("VALUES (");
                sqlb.append(valsSB.toString());
                sqlb.append(')');
            }

            return sqlb.toString();
        }
        
    }

    public class DeleteClause extends UpdateClause {
        public DeleteClause(final String tableName) {
            super(tableName);
        }
        public DeleteClause(final String tableName, final String idColName) {
            super(tableName);
            super.withId(idColName);
            //excludedCols.add(idColName);
        }

        @Override
        public DeleteClause setColumns(List<String> cols) {
            throw new UnsupportedOperationException("Cannot set columns for DELETE clause");
        }

        @Override
        public DeleteClause withId(String idCol) {
            super.withId(idCol);
            return this;
        }

        @Override
        public DeleteClause except(String[] cols) {
            throw new UnsupportedOperationException("Cannot set excluded columns for DELETE clause");
        }
        public DeleteClause except(List<String> cols) {
            throw new UnsupportedOperationException("Cannot set excluded columns for DELETE clause");
        }
        @Override
        public DeleteClause except(String col) {
            throw new UnsupportedOperationException("Cannot set excluded columns for DELETE clause");
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            if (whereClause == null) {
                //Prevent accidental whole-table delete
                throw new IllegalStateException("WHERE Clause needed for delete!!");
            }
            final StringBuilder sqlb = new StringBuilder();
            sqlb.append("DELETE FROM ");
            appendFullTableName(tableName, sqlb);
            sqlb.append(clausesDelimiter);
            sqlb.append(whereClause.build(params, metadata));

            return sqlb.toString();
        }
    }

    public class FromClause extends QueryPart {
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
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("FROM ");
            appendFullTableName(tableName, sqlb);
            if ((this.asName != null) && (!asName.isEmpty())) {
                sqlb.append(" AS ");
                sqlb.append(this.asName);
            }
            return sqlb.toString();
        }
    }
    
    public class JoinClause extends FromClause {
        public static final String INNER = "INNER";
        public static final String LEFT = "LEFT";
        public static final String RIGHT = "RIGHT";
        
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
            onClauses.add(new StaticQueryClause(this, cond));
            return this;
        }
        public JoinClause onIf(boolean active, String onTrueCond) {
            if (!active) {
                return this;
            }
            onClauses.add(new StaticQueryClause(this, onTrueCond));
            return this;
        }
        public JoinClause onIf(Function<SqlParameterSource, Boolean> checkFn, String onTrueCond) {
            onClauses.add(new IfExpr(this, checkFn, new StaticQueryClause(this, onTrueCond)));
            return this;
        }
        public JoinClause onIf(Function<SqlParameterSource, Boolean> checkFn, QueryPart onTrueCond) {
            onClauses.add(new IfExpr(this, checkFn, onTrueCond));
            return this;
        }
        public JoinClause onIfElse(boolean active, String onTrueCond, String onFalseCond) {
            if (!active) {
                return this;
            }
            onClauses.add(new StaticQueryClause(this, onTrueCond));
            return this;
        }
        public JoinClause onIfElse(Function<SqlParameterSource, Boolean> checkFn, String onTrueCond, String onFalseCond) {
            onClauses.add(new IfExpr(this, checkFn,
                new StaticQueryClause(this, onTrueCond),
                new StaticQueryClause(this, onFalseCond)));
            return this;
        }
        public JoinClause onIfElse(Function<SqlParameterSource, Boolean> checkFn, QueryPart onTrueCond, QueryPart onFalseCond) {
            onClauses.add(new IfExpr(this, checkFn, onTrueCond, onFalseCond));
            return this;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append(type);
            sqlb.append(" JOIN ");
            appendFullTableName(tableName, sqlb);
            if ((this.asName != null) && (!asName.isEmpty())) {
                sqlb.append(" AS ");
                sqlb.append(this.asName);
            }
            //sqlb.append(clausesDelimiter);
            sqlb.append(" ON ");
            int nOnClauses = onClauses.size();
            for (int i=0;i < nOnClauses;i++) {
                if (i > 0) {
                    sqlb.append(clausesDelimiter);
                }
                sqlb.append('(');
                sqlb.append(onClauses.get(i).build(params, metadata));
                sqlb.append(')');
            }
            return sqlb.toString();
        }
    }


    public class WhereClause extends QueryPart {
        protected final List<QueryPart> subClauses;
        //protected DatabaseMetaData metaData;
        protected String conj = "AND";

        /*
        public WhereClause setMetaData(DatabaseMetaData metaData) {
            this.metaData = metaData;
            return this;
        }
        */

        public WhereClause setConj(String conj) {
            this.conj = conj;
            return this;
        }

        public WhereClause() {
            this((QueryPart)null);
        }
        public WhereClause(final QueryPart parent) {
            super(parent);
            this.subClauses = new ArrayList<>();
        }
        public WhereClause(List<String> conds) {
            this((QueryPart)null);
            addStrings(conds);
        }
        public WhereClause(QueryPart parent, List<String> conds) {
            this(parent);
            addStrings(conds);
        }

        public WhereClause and(List<String> conds) {
            setConj("AND");
            addStrings(conds);
            return this;
        }
        public WhereClause and(String cond) {
            setConj("AND");
            add(cond);
            return this;
        }
        public WhereClause and(QueryPart cond) {
            setConj("AND");
            add(cond);
            return this;
        }

        public WhereClause or(List<String> conds) {
            setConj("OR");
            addStrings(conds);
            return this;
        }
        public WhereClause or(String cond) {
            setConj("OR");
            add(cond);
            return this;
        }
        public WhereClause or(QueryPart cond) {
            setConj("OR");
            add(cond);
            return this;
        }

        public WhereClause addStrings(List<String> conds) {
            conds.forEach((cond) -> add(cond));
            return this;
        }
        public WhereClause addClauses(List<QueryPart> conds) {
            conds.forEach((cond) -> add(cond));
            return this;
        }
        public WhereClause add(String cond) {
            add(new StaticQueryClause(this, cond));
            return this;
        }
        public WhereClause add(QueryPart cond) {
            subClauses.add(cond);
            return this;
        }

        public WhereClause addIf(boolean active, String onTrueCond) {
            addIf(active, new StaticQueryClause(this, onTrueCond));
            return this;
        }
        public WhereClause addIf(boolean active, QueryPart onTrueCond) {
            if (!active) {
                return this;
            }
            subClauses.add(onTrueCond);
                
            return this;
        }

        public WhereClause addIfElse(boolean active, String onTrueCond, String onFalseCond) {
            addIfElse(active, new StaticQueryClause(this, onTrueCond), new StaticQueryClause(this, onFalseCond));
            return this;
        }
        public WhereClause addIfElse(boolean active, QueryPart onTrueCond, QueryPart onFalseCond) {
            if (active) {
                add(onTrueCond);
            } else {
                add(onFalseCond);
            }
            return this;
        }

        public WhereClause addIf(Function<SqlParameterSource, Boolean> checkFn, String onTrueCond) {
            addIf(checkFn, new StaticQueryClause(this, onTrueCond));
            return this;
        }
        public WhereClause addIf(Function<SqlParameterSource, Boolean> checkFn, QueryPart onTrueCond) {
            subClauses.add(new IfExpr(this, checkFn, onTrueCond));
            return this;
        }

        public WhereClause addIfElse(Function<SqlParameterSource, Boolean> checkFn, String onTrueCond, String onFalseCond) {
            subClauses.add(new IfExpr(this, checkFn,
                new StaticQueryClause(this, onTrueCond),
                new StaticQueryClause(this, onFalseCond)));
            return this;
        }
        public WhereClause addIfElse(Function<SqlParameterSource, Boolean> checkFn, QueryPart onTrueCond, QueryPart onFalseCond) {
            subClauses.add(new IfExpr(this, checkFn, onTrueCond, onFalseCond));
            return this;
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("WHERE ");
            final int n = subClauses.size();
            for (int i=0;i < n;i++) {
                if (i > 0) {
                    sqlb.append(clausesDelimiter);
                    sqlb.append(conj);
                    sqlb.append(' ');
                }
                QueryPart clause = subClauses.get(i);
                if (clause == null) {
                    continue;
                }
                sqlb.append('(');
                sqlb.append(clause.build(params, metadata));
                sqlb.append(')');
            }

            return sqlb.toString();
        }
    }
    public class OnClause extends WhereClause {
        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            StringBuilder sqlb = new StringBuilder();
            sqlb.append("ON ");
            final int n = subClauses.size();
            for (int i=0;i < n;i++) {
                if (i > 0) {
                    sqlb.append(clausesDelimiter);
                    sqlb.append(conj);
                    sqlb.append(' ');
                }
                QueryPart clause = subClauses.get(i);
                if (clause == null) {
                    continue;
                }
                sqlb.append('(');
                sqlb.append(clause.build(params, metadata));
                sqlb.append(')');
            }

            return sqlb.toString();
        }
    }

    public static abstract class AfterWhereClause extends QueryPart {
    }

    public class OrderByClause extends AfterWhereClause {
        protected final List<String> cols;
        protected final List<Boolean> descs;

        public OrderByClause(List<String> cols) {
            this.cols = cols;
            this.descs = new ArrayList<>();
        }
        public OrderByClause(List<String> cols, List<Boolean> descs) {
            this(cols);
            this.descs.clear();
            this.descs.addAll(descs);
        }

        @Override
        public String build(SqlParameterSource params, DatabaseMetaData metadata) throws SQLException {
            final StringBuilder sqlb = new StringBuilder();
            sqlb.append("ORDER BY ");
            final int nCols = cols.size();
            final int nDescs = descs.size();
            for (int i=0;i < nCols;i++) {
                if (i > 0) {
                    sqlb.append(',');
                }
                boolean desc = (i < nDescs) ? descs.get(i) : false;
                sqlb.append(cols.get(i));
                if (desc) {
                    sqlb.append(" DESC");
                }
            }

            return sqlb.toString();
        }
    }

    //TODO: GroupBy & Paging Clauses
}