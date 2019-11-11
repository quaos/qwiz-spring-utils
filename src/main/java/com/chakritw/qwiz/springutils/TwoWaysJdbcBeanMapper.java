package com.chakritw.qwiz.springutils;

import java.beans.PropertyDescriptor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.AbstractSqlParameterSource;

public class TwoWaysJdbcBeanMapper<T> extends AbstractSqlParameterSource implements RowMapper<T> {
    protected final T bean;
    protected final Class<T> beanClass;
    protected final BeanWrapper beanWrapper;
    //protected final Map<String, String> fwdMap;
    protected final Map<String, String> invMap;
    protected final ObjectMapper objectMapper;

    public TwoWaysJdbcBeanMapper(T bean) {
        this.bean = bean;
        this.beanClass = (Class<T>)bean.getClass();
        this.beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(bean);
        //this.fwdMap = new HashMap<>();
        this.invMap = new HashMap<>();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * See: RowMapper<T>.mapRows()
     */
	@Override
	public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<String, String> e : invMap.entrySet()) {
            String colName = e.getKey();
            String propName = e.getValue();
            Object colVal = rs.getObject(colName);
            m.put(propName, colVal);
        }

        return beanWrapper.convertIfNecessary(m, beanClass);
	}

    /**
     * See: AbstractSqlParameterSource.hasValue()
     */
    @Override
    public boolean hasValue(String paramName) {
        return invMap.containsKey(paramName);
    }

    /**
     * See: AbstractSqlParameterSource.getValue()
     */
    @Override
    public Object getValue(String paramName) throws IllegalArgumentException {
        String propName = invMap.get(paramName);
        if (propName == null) {
            throw new IllegalArgumentException("No bean property mapped for column/parameter: " + paramName);
        }

        return beanWrapper.getPropertyValue(propName);
    }
    
    public TwoWaysJdbcBeanMapper<T> map(String propName, String colName) {
        //fwdMap.put(propName, colName);
        invMap.put(colName, propName);
        return this;
    }
}