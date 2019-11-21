package com.chakritw.qwiz.springutils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.AbstractSqlParameterSource;

public class TwoWaysJdbcBeanMapper<T> extends AbstractSqlParameterSource implements RowMapper<T> {
    public final Pattern GETTER_PATTERN = Pattern.compile("^(get|is)([A-Z]{1})([A-Za-z0-9_\\$]*)$");

    protected final T bean;
    protected final Class<T> beanClass;
    protected final BeanWrapper beanWrapper;
    protected final Map<String, String> fwdMap;
    protected final Map<String, String> invMap;
    protected final ObjectMapper objectMapper;

    public TwoWaysJdbcBeanMapper(T bean) {
        this(bean, (Class<T>) bean.getClass());
    }

    public TwoWaysJdbcBeanMapper(Class<T> beanCls) {
        this(null, beanCls);
    }

    public TwoWaysJdbcBeanMapper(T bean, Class<T> refCls) {
        this.bean = bean;
        this.beanClass = refCls;
        this.beanWrapper = (bean != null) ? PropertyAccessorFactory.forBeanPropertyAccess(bean) : null;
        this.fwdMap = new HashMap<>();
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

        return (beanWrapper != null) ? beanWrapper.convertIfNecessary(m, beanClass)
            : objectMapper.convertValue(m, beanClass);
    }

    /**
     * See: SqlParameterSource.getParameterNames()
     */
    @Override
    public String[] getParameterNames() {
        final Collection<String> names = invMap.keySet();
        return names.toArray(new String[names.size()]);
    }

    /**
     * See: SqlParameterSource.hasValue()
     */
    @Override
    public boolean hasValue(String paramName) {
        return invMap.containsKey(paramName);
    }

    /**
     * See: SqlParameterSource.getValue()
     */
    @Override
    public Object getValue(String paramName) throws IllegalArgumentException {
        String propName = invMap.get(paramName);
        if (propName == null) {
            throw new IllegalArgumentException("No bean property mapped for column/parameter: " + paramName);
        }

        return beanWrapper.getPropertyValue(propName);
    }

    public String getMappedName(String propName) {
        return fwdMap.get(propName);
    }

    public TwoWaysJdbcBeanMapper<T> mapSame() {
        mapSameExcept((Collection<String>) null);
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> mapSameExcept(String... excludedProps) {
        mapSameExcept(Arrays.asList(excludedProps));
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> mapSameExcept(Collection<String> excludedProps) {
        final Collection<String> props = getPropNames(excludedProps);
        props.forEach((prop) -> map(prop, prop));
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> map(Function<String, String> ruleFn) {
        final Collection<String> props = getPropNames(null);
        props.forEach((prop) -> {
            Optional.ofNullable(ruleFn.apply(prop))
                .ifPresent((col) -> map(prop, col));
        });
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> map(BiFunction<String, Class<?>, String> ruleFn) {
        final Map<String, Class<?>> propsMap = getPropNamesAndTypesMap(null);
        propsMap.entrySet().forEach((e) -> {
            final String propName = e.getKey();
            Optional.ofNullable(ruleFn.apply(propName, e.getValue()))
                .ifPresent((col) -> map(propName, col));
        });
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> map(Map<String, String> m) {
        m.entrySet().forEach((e) -> map(e.getKey(), e.getValue()));
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> map(Collection<Map.Entry<String, String>> entries) {
        entries.forEach((e) -> map(e.getKey(), e.getValue()));
        return this;
    }

    public TwoWaysJdbcBeanMapper<T> map(String propName, String colName) {
        fwdMap.put(propName, colName);
        invMap.put(colName, propName);
        return this;
    }

    protected Collection<String> getPropNames(Collection<String> excludedProps) {
        Map<String, Class<?>> m = getPropNamesAndTypesMap(excludedProps);
        return m.keySet();
    }

    protected Map<String, Class<?>> getPropNamesAndTypesMap(Collection<String> excludedProps) {
        if (excludedProps == null) {
            excludedProps = new ArrayList<>();
        }
        final Map<String, Class<?>> propsMap = new HashMap<>();
        for (Field fld : beanClass.getFields()) {
            if ((!fld.isAccessible()) && (!Modifier.isPublic(fld.getModifiers()))) {
                continue;
            }
            final String name = fld.getName();
            if (excludedProps.contains(name)) {
                continue;
            }
            propsMap.put(name, fld.getType());
        }
        for (Method method : beanClass.getMethods()) {
            if ((!method.isAccessible()) && (!Modifier.isPublic(method.getModifiers()))) {
                continue;
            }
            final String mName = method.getName();
            Matcher getterMatcher = GETTER_PATTERN.matcher(mName);
            if (!getterMatcher.matches()) {
                continue;
            }
            final int nGroups = getterMatcher.groupCount();
            final String name = getterMatcher.group(2).toLowerCase() + ((nGroups >= 3) ? getterMatcher.group(3) : "");
            if ("class".equals(name)) {
                continue;
            }
            if (excludedProps.contains(name)) {
                continue;
            }
            propsMap.put(name, method.getReturnType());
        }

        return propsMap;
    }

}