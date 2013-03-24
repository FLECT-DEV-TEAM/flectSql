package jp.co.flect.sql;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import javax.swing.event.EventListenerList;
import java.util.Date;
import java.math.BigDecimal;

public abstract class Table implements Selectable {
	
	protected abstract void init();
	
	private Map<String, Object> valueMap = new HashMap<String, Object>();
	private LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<String, Field>();
	private boolean initialized = false;
	private boolean serialKey = false;
	private String tableName;
	
	private EventListenerList listenerList = new EventListenerList();
	
	public Table(String tableName) {
		this(tableName, false);
	}
	
	public Table(String tableName, boolean serialKey) {
		this.tableName = tableName;
		this.serialKey = serialKey;
		doInit();
	}
	
	public Map<String, Object> getValueMap() {
		return new HashMap<String, Object>(this.valueMap);
	}
	
	protected void setValueMap(Map<String, Object> map) {
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			if (hasField(entry.getKey())) {
				this.valueMap.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	/**
	 * テーブル名を返します。
	 */
	public String getTableName() {
		return this.tableName;
	}
	
	/**
	 * PRIMARY KEYが自動生成されるシーケンスかどうかを返します。
	 */
	public boolean useSerialKey() {
		return this.serialKey;
	}
	
	/**
	 * フィールド一覧の取得
	 */
	public List<Field> listFields() {
		return new ArrayList<Field>(fieldMap.values());
	}
	
	public List<Field> listKeys() {
		List<Field> list = new ArrayList();
		for (Field f : fieldMap.values()) {
			if (f.isPrimaryKey()) {
				list.add(f);
			}
		}
		return list;
	}
	
	public List<Field> listValued() {
		List<Field> list = new ArrayList();
		for (Field f : fieldMap.values()) {
			if (valueMap.containsKey(f.getName())) {
				list.add(f);
			}
		}
		return list;
	}
	public int countValued() {
		return valueMap.size();
	}
	
	public boolean hasField(String name) {
		return fieldMap.get(name) != null;
	}
	
	public Field getField(String name) {
		return fieldMap.get(name);
	}
	
	public void addTableListener(TableListener l) {
		listenerList.add(TableListener.class, l);
	}
	
	public void removeTableListener(TableListener l) {
		listenerList.remove(TableListener.class, l);
	}
	
	private void fireEvent(int eventId) {
		TableListener[] ls = listenerList.getListeners(TableListener.class);
		if (ls == null || ls.length == 0) {
			return;
		}
		TableEvent e = new TableEvent(this);
		for (TableListener l : ls) {
			switch (eventId) {
				case TableEvent.BEFORE_INSERT:
					l.beforeInsert(e);
					break;
				case TableEvent.AFTER_INSERT:
					l.afterInsert(e);
					break;
				case TableEvent.BEFORE_UPDATE:
					l.beforeUpdate(e);
					break;
				case TableEvent.AFTER_UPDATE:
					l.afterUpdate(e);
					break;
				case TableEvent.BEFORE_DELETE:
					l.beforeDelete(e);
					break;
				case TableEvent.AFTER_DELETE:
					l.afterDelete(e);
					break;
			}
		}
	}
	
	void fireBeforeInsert() {
		fireEvent(TableEvent.BEFORE_INSERT);
	}
	
	void fireAfterInsert() {
		fireEvent(TableEvent.AFTER_INSERT);
	}
	
	void fireBeforeUpdate() {
		fireEvent(TableEvent.BEFORE_UPDATE);
	}
	
	void fireAfterUpdate() {
		fireEvent(TableEvent.AFTER_UPDATE);
	}
	
	void fireBeforeDelete() {
		fireEvent(TableEvent.BEFORE_DELETE);
	}
	
	void fireAfterDelete() {
		fireEvent(TableEvent.AFTER_DELETE);
	}
	
	private void doInit() {
		beforeInit();
		init();
		afterInit();
		initialized = true;
	}
	
	protected void beforeInit() {}
	protected void afterInit() {}
	
	private void checkName(String name) {
		if (fieldMap.get(name) == null) {
			throw new IllegalArgumentException(name);
		}
	}
	
	protected void addField(String name, int type, boolean primaryKey) {
		if (initialized) {
			throw new IllegalStateException("Already initialized");
		}
		fieldMap.put(name, new Field(name, type, primaryKey));
	}
	
	public Object get(String name) {
		return valueMap.get(name);
	}
	
	public void set(String name, Object value) {
		checkName(name);
		valueMap.put(name, value);
	}
	
	protected String doGetString(String name) {
		return (String)get(name);
	}
	
	protected int doGetInt(String name) {
		Number n = (Number)get(name);
		return n == null ? 0 : n.intValue();
	}
	
	protected long doGetLong(String name) {
		Number n = (Number)get(name);
		return n == null ? 0 : n.longValue();
	}
	
	protected double doGetDouble(String name) {
		Number n = (Number)get(name);
		return n == null ? 0 : n.doubleValue();
	}
	
	protected BigDecimal doGetBigDecimal(String name) {
		Number n = (Number)get(name);
		return n == null ? null : n instanceof BigDecimal ? (BigDecimal)n : new BigDecimal(n.toString());
	}
	
	protected boolean doGetBoolean(String name) {
		Boolean b = (Boolean)get(name);
		return b != null && b.booleanValue();
	}
	
	protected byte[] doGetBytes(String name) {
		return (byte[])get(name);
	}
	
	protected Date doGetDate(String name) {
		return (Date)get(name);
	}
	
	public String buildInsertStatement() {
		if (valueMap.size() == 0) {
			throw new IllegalArgumentException();
		}
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO ")
			.append(getTableName())
			.append(" (");
		
		StringBuilder phBuf = new StringBuilder();
		for (Field f : fieldMap.values()) {
			if (!valueMap.containsKey(f.getName())) {
				continue;
			}
			if (phBuf.length() > 0) {
				buf.append(",");
				phBuf.append(",");
			}
			buf.append(f.getName());
			phBuf.append("?");
		}
		buf.append(") VALUES(")
			.append(phBuf)
			.append(")");
		return buf.toString();
	}
	
	public String buildUpdateStatement(List<Field> whereList) {
		if (valueMap.size() == 0 || whereList == null || whereList.size() == 0) {
			throw new IllegalArgumentException();
		}
		StringBuilder buf = new StringBuilder();
		buf.append("UPDATE ")
			.append(getTableName())
			.append(" SET ");
		
		boolean first = true;
		for (Field f : fieldMap.values()) {
			if (whereList.contains(f)) {
				Object o = valueMap.get(f.getName());
				if (o == null) {
					throw new IllegalArgumentException();
				}
			} else if (valueMap.containsKey(f.getName())) {
				if (!first) {
					buf.append(",");
				}
				buf.append(f.getName()).append(" = ?");
				first = false;
			}
		}
		buf.append(" WHERE ");
		first = true;
		for (Field f : whereList) {
			if (!first) {
				buf.append(" AND ");
			}
			buf.append(f.getName()).append(" = ?");
			first = false;
		}
		return buf.toString();
	}
	
	public String buildDeleteStatement(List<Field> whereList) {
		if (whereList == null || whereList.size() == 0) {
			throw new IllegalArgumentException();
		}
		StringBuilder buf = new StringBuilder();
		buf.append("DELETE FROM ")
			.append(getTableName())
			.append(" WHERE ");
		boolean first = true;
		for (Field f : whereList) {
			if (!first) {
				buf.append(" AND ");
			}
			buf.append(f.getName()).append(" = ?");
			first = false;
		}
		return buf.toString();
		
	}
	
	@Override
	public String toString() {
		return getTableName();
	}
	
	public static class Field {
		
		private int type;
		private String name;
		private boolean primaryKey;
		
		public Field(String name, int type, boolean primaryKey) {
			this.name = name;
			this.type = type;
			this.primaryKey = primaryKey;
		}
		
		public String getName() { return this.name;}
		public int getType() { return this.type;}
		public boolean isPrimaryKey() { return this.primaryKey;}
		
		public boolean equals(Object o) {
			if (o instanceof Field) {
				Field f = (Field)o;
				return f.name.equals(name) && f.type == type && f.primaryKey == primaryKey;
			}
			return false;
		}
		
		public int hashCode() {
			return name.hashCode();
		}
	}
}

