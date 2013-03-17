package jp.co.flect.sql;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import jp.co.flect.sql.Condition.LogicalOp;
import jp.co.flect.sql.Condition.ComparisionOp;
import jp.co.flect.sql.Condition.CompoundCondition;
import jp.co.flect.sql.Condition.Combine;

/**
 * SELECT文を構築するクラス<br>
 * 各種メソッドでテーブル(Selectable)を指定する場合はそのインスタンスは
 * コンストラクタまたはjoinで指定されていなければなりません。
 */
public class SelectBuilder implements Selectable {
	
	/** offsetにパラメータを使用することを示す定数 */
	public static final int OFFSET_PARAM = -1;
	/** limitにパラメータを使用することを示す定数 */
	public static final int LIMIT_PARAM = -1;
	
	private SelectBuilder parent;
	
	private String aliasPrefix = "";
	private char quoteChar = 0;
	private TableInfo mainTable;
	private List<Select> selects = new ArrayList<Select>();
	private List<Join> joins = null;
	private Where where = null;
	private List<OrderByEntry> orderBy = null;
	private List<Column> groupBy = null;
	private ForUpdate forUpdate = null;
	
	private boolean distinct = false;
	private int limit = 0;
	private int offset = 0;
	
	/** ResultSetのデータ型 */
	private int[] rsTypes = null;
	
	/**
	 * FROM句に指定する主となるテーブルを引数としてSelectBuilderを構築します
	 */
	public SelectBuilder(Selectable table) {
		this.mainTable = new TableInfo(table, "A");
	}
	
	/**
	 * FROM句に指定された主となるテーブルを返します
	 */
	public Selectable getMainTable() {
		return mainTable.getTable();
	}
	
	/**
	 * SELECT DISTINCTにします
	 */
	public SelectBuilder distinct() {
		return distinct(true);
	}
	
	/**
	 * SELECT DISTINCTにするかどうかを設定します
	 */
	public SelectBuilder distinct(boolean b) {
		this.distinct = b;
		return this;
	}
	
	/**
	 * SELECT句にフィールドを追加します<br>
	 * @param field フィールド名 または任意のリテラル
	 */
	public SelectBuilder select(String field) {
		Selectable t = searchTable(field);
		return select(new Column(t, field));
	}
	
	/**
	 * SELECT句にフィールドを追加します<br>
	 * @param table フィールドを保持するテーブル。
	 * @param field フィールド名
	 */
	public SelectBuilder select(Selectable table, String field) {
		return select(new Column(table, field));
	}
	
	/**
	 * SELECT句にフィールドを追加します<br>
	 * @param sel フィールド
	 */
	public SelectBuilder select(Select sel) {
		this.selects.add(sel);
		this.rsTypes = null;
		return this;
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(昇順)<br>
	 * @param field フィールド名 または任意のリテラル
	 */
	public SelectBuilder orderByAsc(String field) {
		return orderBy(field, true);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(降順)<br>
	 * @param field フィールド名 または任意のリテラル
	 */
	public SelectBuilder orderByDesc(String field) {
		return orderBy(field, false);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します<br>
	 * @param field フィールド名 または任意のリテラル
	 * @param asc 昇順の場合true
	 */
	public SelectBuilder orderBy(String field, boolean asc) {
		Selectable t = searchTable(field);
		return orderBy(new Column(t, field), asc);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(昇順)<br>
	 * @param table フィールドを保持するテーブル。
	 * @param field フィールド名
	 */
	public SelectBuilder orderByAsc(Selectable t, String field) {
		return orderBy(t, field, true);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(降順)<br>
	 * @param table フィールドを保持するテーブル。
	 * @param field フィールド名
	 */
	public SelectBuilder orderByDesc(Selectable t, String field) {
		return orderBy(t, field, false);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します<br>
	 * @param table フィールドを保持するテーブル。
	 * @param field フィールド名
	 * @param asc 昇順の場合true
	 */
	public SelectBuilder orderBy(Selectable t, String field, boolean asc) {
		return orderBy(new Column(t, field), asc);
	}
	
	/**
	 * ORDER BY句にフィールド番号を追加します(昇順)<br>
	 * @param colNo SELECT句のフィールド番号
	 */
	public SelectBuilder orderByAsc(int colNo) {
		return orderBy(colNo, true);
	}
	
	/**
	 * ORDER BY句にフィールド番号を追加します(降順)<br>
	 * @param colNo SELECT句のフィールド番号
	 */
	public SelectBuilder orderByDesc(int colNo) {
		return orderBy(colNo, false);
	}
	
	/**
	 * ORDER BY句にフィールド番号を追加します<br>
	 * @param colNo SELECT句のフィールド番号
	 * @param asc 昇順の場合true
	 */
	public SelectBuilder orderBy(int colNo, boolean asc) {
		return orderBy(new Column(null, Integer.toString(colNo)), asc);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(昇順)<br>
	 * @param sel フィールド
	 */
	public SelectBuilder orderByAsc(Select sel) {
		return orderBy(sel, true);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します(降順)<br>
	 * @param sel フィールド
	 */
	public SelectBuilder orderByDesc(Select sel) {
		return orderBy(sel, false);
	}
	
	/**
	 * ORDER BY句にフィールドを追加します<br>
	 * @param sel フィールド
	 * @param asc 昇順の場合true
	 */
	public SelectBuilder orderBy(Select sel, boolean asc) {
		if (this.orderBy == null) {
			this.orderBy = new ArrayList<OrderByEntry>();
		}
		this.orderBy.add(new OrderByEntry(sel, asc));
		return this;
	}
	
	/**
	 * GROUP BY句にフィールドを追加します<br>
	 * @param sel フィールド
	 */
	public SelectBuilder groupBy(String field) {
		Selectable t = searchTable(field);
		return groupBy(new Column(t, field));
	}
	
	/**
	 * GROUP BY句にフィールドを追加します<br>
	 * @param table フィールドを保持するテーブル。
	 * @param sel フィールド
	 */
	public SelectBuilder groupBy(Selectable table, String field) {
		return groupBy(new Column(table, field));
	}
	
	/**
	 * GROUP BY句にフィールドを追加します<br>
	 * @param col フィールド
	 */
	public SelectBuilder groupBy(Column col) {
		if (this.groupBy == null) {
			this.groupBy = new ArrayList<Column>();
		}
		this.groupBy.add(col);
		return this;
	}
	
	/**
	 * OFFSET句にパラメータを使用します
	 */
	public SelectBuilder offset() {
		return offset(OFFSET_PARAM);
	}
	
	/**
	 * OFFSET句を設定します
	 */
	public SelectBuilder offset(int n) {
		this.offset = n;
		return this;
	}
	
	/**
	 * LIMIT句にパラメータを使用します
	 */
	public SelectBuilder limit() {
		return limit(LIMIT_PARAM);
	}
	
	/**
	 * LIMIT句を設定します
	 */
	public SelectBuilder limit(int n) {
		this.limit = n;
		return this;
	}
	
	/**
	 * 引数のtableとINNER JOINします
	 * @param table テーブル
	 */
	public SelectBuilder innerJoin(Selectable table) {
		return addJoin(new InnerJoin(table, calcTableAlias()));
	}
	
	/**
	public SelectBuilder innerJoin(Selectable table, String alias) {
		return addJoin(new InnerJoin(table, alias));
	}
	*/
	
	/**
	 * 引数のtableとLEFT JOINします
	 * @param table テーブル
	 */
	public SelectBuilder leftJoin(Selectable table) {
		return addJoin(new LeftJoin(table, calcTableAlias()));
	}
	
	/*
	public SelectBuilder leftJoin(Selectable table, String alias) {
		return addJoin(new LeftJoin(table, alias));
	}
	*/
	
	/**
	 * 引数のtableとRIGHT JOINします
	 * @param table テーブル
	 */
	public SelectBuilder rightJoin(Selectable table) {
		return addJoin(new RightJoin(table, calcTableAlias()));
	}
	
	/*
	public SelectBuilder rightJoin(Selectable table, String alias) {
		return addJoin(new RightJoin(table, alias));
	}
	*/
	
	private SelectBuilder addJoin(Join join) {
		if (this.joins == null) {
			this.joins = new ArrayList<Join>();
		}
		this.joins.add(join);
		return this;
	}
	
	/**
	 * 最後に結合されたJoinを返します。
	 */
	public Join getCurrentJoin() {
		return joins == null ? null : joins.get(joins.size()-1);
	}
	
	/**
	 * 指定のフィールド名で最後に結合されたJoinのON句を設定します。<br>
	 * フィールド名は新たに結合されたテーブルと既存のテーブルの
	 * 両方に含まれている必要があります。<br>
	 * @param field フィールド名
	 */
	public SelectBuilder on(String field) {
		return getCurrentJoin().on(field);
	}
	
	/**
	 * 指定のフィールド名で最後に結合されたJoinのON句を設定します。<br>
	 * @param field1 新たに結合されたテーブルのフィールド名
	 * @param field2 既存のテーブルのフィールド名
	 */
	public SelectBuilder on(String field1, String field2) {
		return getCurrentJoin().on(field1, field2);
	}
	
	/**
	 * 指定のフィールド名で最後に結合されたJoinのON句を設定します。<br>
	 * @param table1 結合に使用するテーブル1
	 * @param field1 テーブル1のフィールド名
	 * @param table2 結合に使用するテーブル2
	 * @param field2 テーブル2のフィールド名
	 */
	public SelectBuilder on(Selectable table1, String field1, Selectable table2, String field2) {
		return getCurrentJoin().on(table1, field1, table2, field2);
	}
	
	/**
	 * 指定のフィールド名で最後に結合されたJoinのON句を設定します。<br>
	 * @param table1 結合に使用するテーブル1
	 * @param field1 テーブル1のフィールド名
	 * @param table2 結合に使用するテーブル2
	 * @param field2 テーブル2のフィールド名
	 * @param op     結合に使用する比較演算子
	 */
	public SelectBuilder on(Selectable table1, String field1, Selectable table2, String field2, ComparisionOp op) {
		return getCurrentJoin().on(table1, field1, table2, field2, op);
	}
	
	/**
	 * AND条件でWHERE句に条件を追加します。<br>
	 * (andメソッドと処理内容は同じです。)
	 * @param cond 条件
	 */
	public SelectBuilder where(Condition cond) {
		if (where == null) {
			where = new Where(cond);
		} else {
			where.and(cond);
		}
		return this;
	}
	
	/**
	 * AND条件でWHERE句に条件を追加します。<br>
	 * @param cond 条件
	 */
	public SelectBuilder and(Condition cond) {
		if (where == null) {
			where = new Where(cond);
		} else {
			where.and(cond);
		}
		return this;
	}
	
	/**
	 * OR条件でWHERE句に条件を追加します。<br>
	 * @param cond 条件
	 */
	public SelectBuilder or(Condition cond) {
		if (where == null) {
			where = new Where(cond);
		} else {
			where.or(cond);
		}
		return this;
	}
	
	/**
	 * FOR UPDATE を指定します。
	 */
	public SelectBuilder forUpdate() {
		this.forUpdate = new ForUpdate(false);
		return this;
	}
	
	/**
	 * FOR UPDATE NOWAIT を指定します。
	 */
	public SelectBuilder forUpdateNoWait() {
		this.forUpdate = new ForUpdate(true);
		return this;
	}
	/**
	 * テーブル名やフィールド名をクォートする場合そのクォート文字を返します。
	 */
	public char getQuoteChar() { return this.quoteChar;}
	
	/**
	 * テーブル名やフィールド名をクォートする場合そのクォート文字を設定します。
	 */
	public void setQuoteChar(char c) { this.quoteChar = c;}
	
	/**
	 * SQLを返します。<br>
	 * 設定内容が不正な場合はIllegalArgumentExceptionとなります。
	 */
	public String toSQL() {
		return toSQL(false);
	}
	
	/**
	 * 改行されたSQLを返します。<br>
	 * 設定内容が不正な場合はIllegalArgumentExceptionとなります。
	 */
	public String toSQL(boolean newLine) {
		StringBuilder buf = new StringBuilder();
		build(buf, newLine);
		return buf.toString();
	}
	
	/**
	 * 改行されたSQLを返します。<br>
	 * 設定内容が不正な場合は構築できたところまでのSQL文を返します。
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		try {
			build(buf, true);
			return buf.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return "Invalid SELECT: " + buf;
		}
	}
	
	/**
	 * ResultSetの現在の行の内容をMapにコピーします。
	 */
	public Map<String, Object> map(ResultSet rs) throws SQLException {
		if (this.rsTypes == null) {
			ResultSetMetaData meta = rs.getMetaData();
			if (meta.getColumnCount() < selects.size()) {
				throw new IllegalArgumentException("Invalid resultSet");
			}
			int[] temp = new int[selects.size()];
			for (int i=0; i<selects.size(); i++) {
				temp[i] = meta.getColumnType(i+1);
			}
			this.rsTypes = temp;
		}
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i=0; i<selects.size(); i++) {
			Select sel = selects.get(i);
			int sqlType = rsTypes[i];
			Object value = null;
			switch (sqlType) {
				case Types.BIGINT:
				case Types.ROWID:
					value = rs.getLong(i+1);
					break;
				case Types.BLOB:
				case Types.BINARY:
					value = rs.getBytes(i+1);
					break;
				case Types.BIT:
				case Types.BOOLEAN:
					value = rs.getBoolean(i+1);
					break;
				case Types.CHAR:
				case Types.CLOB:
				case Types.NCHAR:
				case Types.NCLOB:
				case Types.NVARCHAR:
				case Types.SQLXML:
				case Types.VARCHAR:
					value = rs.getString(i+1);
					break;
				case Types.DATE:
					value = rs.getDate(i+1);
					break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					value = rs.getBigDecimal(i+1);
					break;
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.REAL:
					value = rs.getDouble(i+1);
					break;
				case Types.INTEGER:
				case Types.SMALLINT:
				case Types.TINYINT:
					value = rs.getInt(i+1);
					break;
				case Types.NULL:
					value = null;
					break;
				case Types.TIME:
					value = rs.getTime(i+1);
					break;
				case Types.TIMESTAMP:
					value = rs.getTimestamp(i+1);
					break;
				case Types.OTHER:
				case Types.REF:
				case Types.STRUCT:
				case Types.VARBINARY:
					break;
				case Types.ARRAY:
				case Types.DATALINK:
				case Types.DISTINCT:
				case Types.JAVA_OBJECT:
				case Types.LONGNVARCHAR:
				case Types.LONGVARBINARY:
				case Types.LONGVARCHAR:
				default:
					throw new IllegalArgumentException("UnsupportedType: " + sqlType);
			}
			if (rs.wasNull()) {
				value = null;
			}
			map.put(sel.getFieldName(), value);
		}
		return map;
	}
	
	/**
	 * 指定のフィールド名がSELECT句に含まれているかどうかを返します。
	 */
	public boolean hasField(String field) {
		for (Select sel : this.selects) {
			if (sel.getFieldName().equals(field)) {
				return true;
			}
		}
		return false;
	}
	
	SelectBuilder getParent() { return this.parent;}
	void setParent(SelectBuilder builder) { this.parent = builder;}
	
	protected String getAliasPrefix() { return this.aliasPrefix;}
	void setAliasPrefix(String s) { this.aliasPrefix = s;}
	
	Selectable searchTable(String field) {
		Selectable ret = null;
		List<Column> checkList = null;
		if (mainTable.getTable().hasField(field)) {
			ret = mainTable.getTable();
		}
		if (joins != null) {
			for (int i=0; i<joins.size(); i++) {
				TableInfo info = joins.get(i).getTableInfo();
				if (info.getTable().hasField(field)) {
					if (ret == null) {
						ret = info.getTable();
					} else {
						if (checkList == null) {
							checkList = new ArrayList<Column>();
							checkList.add(new Column(ret, field));
						}
						checkList.add(new Column(info.getTable(), field));
					}
				}
			}
		}
		if (ret == null) {
			return this.parent != null ? this.parent.searchTable(field) : null;
		}
		if (checkList != null) {
			for (Column col : checkList) {
				if (!isJoinedColumn(col)) {
					throw new IllegalArgumentException("Ambiguous reference: " + field);
				}
			}
		}
		return ret;
	}
	
	List<Condition> getConditions() {
		if (this.where == null) {
			return null;
		} 
		List<Condition> list = new ArrayList<Condition>();
		for (WhereEntry entry : where.getList()) {
			list.add(entry.cond);
		}
		return list;
	}
	
	private boolean isJoinedColumn(Column col) {
		if (joins != null) {
			for (Join join : joins) {
				if (join.isJoinedColumn(col)) {
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean contains(Selectable t) {
		if (mainTable.getTable() == t) {
			return true;
		}
		if (joins != null) {
			for (Join join : joins) {
				TableInfo info = join.getTableInfo();
				if (info.getTable() == t) {
					return true;
				}
			}
		}
		return false;
	}
	
	private String getTableAlias(Selectable t) {
		if (t == mainTable.getTable()) {
			return mainTable.getAlias();
		}
		if (joins != null) {
			for (int i=0; i<joins.size(); i++) {
				TableInfo info = joins.get(i).getTableInfo();
				if (info.getTable() == t) {
					return info.getAlias();
				}
			}
		}
		if (parent != null) {
			return parent.getTableAlias(t);
		}
		return null;
	}
	
	/*
	private Selectable getTable(String alias) {
		if (alias.equals(mainTable.getAlias())) {
			return mainTable.getTable();
		}
		if (joins != null) {
			for (int i=0; i<joins.size(); i++) {
				TableInfo info = joins.get(i).getTableInfo();
				if (alias.equals(info.getAlias())) {
					return info.getTable();
				}
			}
		}
		if (parent != null) {
			return parent.getTable(alias);
		}
		return null;
	}
	*/
	
	private String calcTableAlias() {
		int prefixLen = aliasPrefix.length();
		char c = 0;
		String alias = mainTable.getAlias().substring(prefixLen);
		if (alias.length() == 1) {
			c = alias.charAt(0);
		}
		if (joins != null) {
			for (Join join : joins) {
				alias = join.getTableInfo().getAlias().substring(prefixLen);
				if (alias.length() == 1) {
					char c2 = alias.charAt(0);
					if (c2 > c) {
						c = c2;
					}
				}
			}
		}
		return c == 0 ? "A" : "" + (char)(c + 1);
	}
	
	private Selectable getTableByFieldName(String field) {
		if (mainTable.getTable().hasField(field)) {
			return mainTable.getTable();
		}
		if (joins != null) {
			for (Join join : joins) {
				TableInfo info = join.getTableInfo();
				if (info.getTable().hasField(field)) {
					return info.getTable();
				}
			}
		}
		return null;
	}
	
	private void quote(StringBuilder buf, String str) {
		if (quoteChar == 0) {
			buf.append(str);
		} else {
			buf.append(quoteChar);
			buf.append(str);
			buf.append(quoteChar);
		}
	}
	
	private void build(StringBuilder buf, boolean newLine) {
		if (selects.size() == 0) {
			throw new IllegalArgumentException("No select clause");
		}
		boolean bGroup = groupBy != null && groupBy.size() > 0;
		buf.append("SELECT ");
		if (distinct) {
			buf.append("DISTINCT ");
		}
		boolean first = true;
		for (Select sel : selects) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			if (newLine) {
				buf.append("\n  ");
			}
			sel.build(buf, this);
			if (sel instanceof Aggregate) {
				bGroup = true;
			}
		}
		if (newLine) {
			buf.append("\n");
		} else {
			buf.append(" ");
		}
		buf.append("FROM ");
		mainTable.build(buf);
		if (joins != null) {
			for (Join join : joins) {
				if (newLine) {
					buf.append("\n");
				} else {
					buf.append(" ");
				}
				join.build(buf);
			}
		}
		if (where != null) {
			where.build(buf, newLine);
		}
		if (bGroup) {
			List<Column> gList = groupBy != null ? groupBy : getGroupByColumn(selects);
			if (gList.size() > 0) {
				if (newLine) {
					buf.append("\n");
				} else {
					buf.append(" ");
				}
				buf.append("GROUP BY ");
				first = true;
				for (Column col : gList) {
					if (first) {
						first = false;
					} else {
						buf.append(", ");
					}
					if (newLine) {
						buf.append("\n  ");
					}
					col.doBuild(buf, this);
				}
			}
		}
		if (orderBy != null) {
			if (newLine) {
				buf.append("\n");
			} else {
				buf.append(" ");
			}
			buf.append("ORDER BY ");
			first = true;
			for (OrderByEntry entry : orderBy) {
				if (first) {
					first = false;
				} else {
					buf.append(", ");
				}
				if (newLine) {
					buf.append("\n  ");
				}
				entry.sel.doBuild(buf, this);
				if (!entry.asc) {
					buf.append(" DESC");
				}
			}
		}
		if (offset != 0) {
			if (newLine) {
				buf.append("\n");
			} else {
				buf.append(" ");
			}
			buf.append("OFFSET ").append(offset == OFFSET_PARAM ? "?" : Integer.toString(offset));
		}
		if (limit != 0) {
			if (newLine) {
				buf.append("\n");
			} else {
				buf.append(" ");
			}
			buf.append("LIMIT ").append(limit == LIMIT_PARAM ? "?" : limit == 0 ? "ALL" : Integer.toString(limit));
		}
		if (forUpdate != null) {
			buf.append(forUpdate);
		}
	}
	
	private List<Column> getGroupByColumn(List<Select> list) {
		List<Column> ret = new ArrayList<Column>();
		for (Select sel : list) {
			if (sel instanceof Column) {
				ret.add((Column)sel);
			}
		}
		return ret;
	}
	
	/**
	 * SELECT句の抽象クラス
	 */
	public static abstract class Select {
		
		private String alias;
		
		public Select alias(String s) { 
			this.alias = s;
			return this;
		}
		
		public String alias() { return this.alias;}
		
		public Select as(String s) {
			return alias(s);
		}
		
		public String as() { return alias();}
		
		public String getFieldName() {
			if (this.alias != null) {
				return this.alias;
			}
			return doGetFieldName();
		}
		
		protected abstract String doGetFieldName();
		public final void build(StringBuilder buf, SelectBuilder builder) {
			doBuild(buf, builder);
			String a = alias();
			if (a != null) {
				buf.append(" AS ");
				builder.quote(buf, a);
			}
		}
		protected abstract void doBuild(StringBuilder buf, SelectBuilder builder);
	}
	
	/**
	 * SELECT句で使用する任意のリテラル
	 */
	public static class Literal extends Select {
		
		private Object literal;
		
		public Literal(Object literal) {
			this.literal = literal;
		}
		
		protected String doGetFieldName() {
			return literal.toString();
		}
		
		protected void doBuild(StringBuilder buf, SelectBuilder builder) {
			buf.append(this.literal);
		}
		
		public static final Literal TRUE = new Literal(true);
		public static final Literal FALSE = new Literal(false);
	}
	
	/**
	 * SELECT句で使用するフィールド
	 */
	public static class Column extends Select {
		
		private Selectable table;
		private String field;
		
		public Column(String f) {
			this(null, f);
		}
		
		public Column(Selectable t, String f) {
			this.table = t;
			this.field = f;
		}
		
		protected String doGetFieldName() {
			return field;
		}
		
		protected void doBuild(StringBuilder buf, SelectBuilder builder) {
			Selectable t = this.table;
			String ta = null;
			if (t == null) {
				t = builder.searchTable(this.field);
			}
			if (t != null) {
				ta = builder.getTableAlias(t);
				if (ta == null) {
					throw new IllegalArgumentException("Unknown table: " + t);
				}
			}
			if (ta != null) {
				builder.quote(buf, ta);
				buf.append(".");
			}
			builder.quote(buf, field);
		}
		
		public Selectable getTable() { return this.table;}
		void setTable(Selectable t) { this.table = t;}
		
		public String getField() { return this.field;}
		void setField(String s) { this.field = s;}
	}
	
	/**
	 * 関数
	 */
	public static class Function extends Select {
		
		private String functionName;
		private Object[] args;
		
		public Function(String functionName, Object... args) {
			this.functionName = functionName;
			this.args = args;
		}
		
		protected String doGetFieldName() {
			return this.functionName;
		}
		
		protected void doBuild(StringBuilder buf, SelectBuilder builder) {
			buf.append(this.functionName)
				.append("(");
			if (this.args != null) {
				boolean first = true;
				for (Object o : args) {
					if (first) {
						first = false;
					} else {
						buf.append(", ");
					}
					appendObject(buf, builder, o);
				}
			}
			buf.append(")");
		}
	}
	
	/**
	 * 集合関数
	 */
	public static class Aggregate extends Function {
		
		public Aggregate(String functionName, Object... args) {
			super(functionName, args);
		}
	}
	
	/**
	 * Case
	 */
	public static class Case extends Select {
		
		private List<CaseEntry> list = new ArrayList<CaseEntry>();
		private Select elseValue;
		
		public Case when(Condition cond, Select value) {
			this.list.add(new CaseEntry(cond, value));
			return this;
		}
		
		public Case caseElse(Select value) {
			elseValue = value;
			return this;
		}
		
		@Override
		protected String doGetFieldName() {
			return "CASE";
		}
		
		@Override
		protected void doBuild(StringBuilder buf, SelectBuilder builder) {
			buf.append("CASE ");
			for (CaseEntry entry : list) {
				buf.append("WHEN ");
				entry.cond.build(buf, builder);
				buf.append(" THEN ");
				entry.value.build(buf, builder);
			}
			if (elseValue != null) {
				buf.append(" ELSE ");
				elseValue.build(buf, builder);
			}
			buf.append(" END");
		}
	}
	
	/**
	 * JOIN句の抽象クラス
	 */
	public abstract class Join {
		
		private TableInfo info;
		private CompoundCondition on = new CompoundCondition(LogicalOp.AND);
		
		public Join(Selectable t, String a) {
			if (a == null) {
				a = calcTableAlias();
			}
			this.info = new TableInfo(t, a);
		}
		
		public SelectBuilder on(String field) {
			return on(field, field);
		}
		
		public SelectBuilder on(String field1, String field2) {
			if (!info.getTable().hasField(field1)) {
				if (!info.getTable().hasField(field2)) {
					throw new IllegalArgumentException("Both " + field1 + " and " + field2 + " are not field of " + info.getTable()); 
				} else {
					return on(field2, field1);
				}
			}
			Selectable table1 = info.getTable();
			Selectable table2 = getTableByFieldName(field2);
			if (table2 == null || table2 == table1) {
				throw new IllegalArgumentException("Invalid field: " + field2);
			}
			return on(table1, field1, table2, field2, ComparisionOp.Equal);
		}
		
		public SelectBuilder on(Selectable table1, String field1, Selectable table2, String field2) {
			return on(table1, field1, table2, field2, ComparisionOp.Equal);
		}
		
		public SelectBuilder on(Selectable table1, String field1, Selectable table2, String field2, ComparisionOp op) {
			Column col1 = new Column(table1, field1);
			Column col2 = new Column(table2, field2);
			on.add(new Combine(col1, col2, op));
			return SelectBuilder.this;
		}
		
		//同一のフィールド名で他のテーブルとJoinしてる場合true
		public boolean isJoinedColumn(Column col) {
			for (Condition c : on.getList()) {
				if (c instanceof Combine) {
					Combine combi = (Combine)c;
					if (combi.getOp() == ComparisionOp.Equal &&
					    combi.getCol1() instanceof Column &&
					    combi.getCol2() instanceof Column)
					{
						Column cc1 = (Column)combi.getCol1();
						Column cc2 = (Column)combi.getCol2();
						if ((cc1.getTable() == col.getTable() || cc2.getTable() == col.getTable()) &&
						    cc1.getField().equals(col.getField()) &&
						    cc2.getField().equals(col.getField()))
						{
							return true;
						}
					}
				}
			}
			return false;
		}
		
		public abstract String getJoinString();
		
		public TableInfo getTableInfo() {
			return this.info;
		}
		
		public void build(StringBuilder buf) {
			buf.append(getJoinString()).append(" ");
			info.build(buf);
			if (on.size() > 0) {
				buf.append(" ON ");
				on.build(buf, SelectBuilder.this, false);
			}
		}
	}
	
	private class InnerJoin extends Join {
		
		public InnerJoin(Selectable t, String a) {
			super(t, a);
		}
		
		public String getJoinString() {
			return "INNER JOIN";
		}
	}
	
	private class LeftJoin extends Join {
		
		public LeftJoin(Selectable t, String a) {
			super(t, a);
		}
		
		public String getJoinString() {
			return "LEFT JOIN";
		}
	}
	
	private class RightJoin extends Join {
		
		public RightJoin(Selectable t, String a) {
			super(t, a);
		}
		
		public String getJoinString() {
			return "RIGHT JOIN";
		}
	}
	
	private class TableInfo {
		
		private Selectable table;
		private String alias;
		
		public TableInfo (Selectable t, String a) {
			this.table = t;
			this.alias = a;
		}
		
		public void build(StringBuilder buf) {
			if (table instanceof Table) {
				quote(buf, ((Table)table).getTableName());
			} else if (table instanceof SelectBuilder) {
				buf.append("(").append(((SelectBuilder)table).toSQL()).append(")");
			} else {
				throw new IllegalStateException();
			}
			buf.append(" ");
			quote(buf, getAlias());
		}
		
		public Selectable getTable() { return this.table;}
		public String getAlias() { return SelectBuilder.this.aliasPrefix + this.alias;}
	}
	
	private class Where {
		
		private List<WhereEntry> list = new ArrayList<WhereEntry>();
		
		public Where(Condition cond) {
			and(cond);
		}
		
		public List<WhereEntry> getList() { return this.list;}
		
		public SelectBuilder and(Condition cond) {
			return doAdd(cond, LogicalOp.AND);
		}
		
		public SelectBuilder or(Condition cond) {
			return doAdd(cond, LogicalOp.OR);
		}
		
		private SelectBuilder doAdd(Condition cond, LogicalOp op) {
			list.add(new WhereEntry(cond, op));
			return SelectBuilder.this;
		}
		
		public void build(StringBuilder buf, boolean newLine) {
			for (int i=0; i<list.size(); i++) {
				WhereEntry entry = list.get(i);
				if (newLine) {
					buf.append("\n");
				} else {
					buf.append(" ");
				}
				if (i == 0) {
					buf.append("WHERE ");
				} else {
					buf.append(entry.op).append(" ");
				}
				entry.cond.build(buf, SelectBuilder.this);
			}
		}
	}
	
	private static class WhereEntry {
		
		private Condition cond;
		private LogicalOp op;
		
		public WhereEntry(Condition cond, LogicalOp op) {
			this.cond = cond;
			this.op = op;
		}
	}
	
	private static class OrderByEntry {
		
		private Select sel;
		private boolean asc;
		
		public OrderByEntry(Select sel, boolean asc) {
			this.sel = sel;
			this.asc = asc;
		}
	}
	
	public static class CaseEntry {
		
		public Condition cond;
		public Select value;
		
		public CaseEntry(Condition cond, Select value) {
			this.cond = cond;
			this.value = value;
		}
	}
	
	private static class ForUpdate {
		
		private boolean nowait;
		
		public ForUpdate(boolean nowait) {
			this.nowait = nowait;
		}
		
		public String toString() {
			String ret = " FOR UPDATE";
			if (nowait) {
				ret += " NOWAIT";
			}
			return ret;
		}
	}
	
	static void appendObject(StringBuilder buf, SelectBuilder builder, Object value) {
		if (value instanceof String) {
			String str = value.toString();
			buf.append("'");
			for (int i=0; i<str.length(); i++) {
				char c = str.charAt(i);
				if (c == '\'') {
					buf.append("''");
				} else {
					buf.append(c);
				}
			}
			buf.append("'");
		} else if (value instanceof Date) {
			buf.append("'")
				.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date)value))
				.append("'");
		} else if (value instanceof Select) {
			((Select)value).build(buf, builder);
		} else {
			buf.append(value);
		}
	}
}
