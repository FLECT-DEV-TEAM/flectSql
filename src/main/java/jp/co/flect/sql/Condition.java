package jp.co.flect.sql;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import jp.co.flect.sql.SelectBuilder.Select;
import jp.co.flect.sql.SelectBuilder.Column;
import jp.co.flect.sql.SelectBuilder.Literal;

/**
 * WHERE条件の抽象クラス
 */
public abstract class Condition {
	
	/**
	 * 比較演算子
	 */
	public enum ComparisionOp {
		Equal("="),
		LessThan("<"),
		LessEqual("<="),
		GreaterThan(">"),
		GreaterEqual(">="),
		NotEqual("<>"),
		Like("LIKE"),
		In("IN"),
		IsNull("is null"),
		IsNotNull("is not null")
		;
		
		private String op;
		
		private ComparisionOp(String op) {
			this.op = op;
		}
		
		public String toString() { return this.op;}
	}
	
	/**
	 * 論理演算子
	 */
	public enum LogicalOp {
		AND,
		OR
	};
	
	public abstract void build(StringBuilder buf, SelectBuilder builder);
	
	/**
	 * AND または OR で結合された条件
	 */
	public static class CompoundCondition extends Condition {
		
		private LogicalOp op;
		private List<Condition> list = new ArrayList<Condition>();
		
		public CompoundCondition() {
			this(LogicalOp.AND);
		}
		
		public CompoundCondition(LogicalOp op) {
			this.op = op;
		}
		
		public CompoundCondition(LogicalOp op, Condition c1, Condition c2) {
			this(op);
			add(c1);
			add(c2);
		}
		
		public LogicalOp getOp() { return this.op;}
		
		public List<Condition> getList() {
			return this.list;
		}
		
		public CompoundCondition add(Condition c) {
			list.add(c);
			return this;
		}
		
		public int size() { return this.list.size();}
		
		public void build(StringBuilder buf, SelectBuilder builder) {
			build(buf, builder, true);
		}
		
		public void build(StringBuilder buf, SelectBuilder builder, boolean bracket) {
			if (bracket) {
				buf.append("(");
			}
			boolean first = true;
			for (Condition c : list) {
				if (first) {
					first = false;
				} else {
					buf.append(" ").append(op).append(" ");
				}
				c.build(buf, builder);
			}
			if (bracket) {
				buf.append(")");
			}
		}
	}
	
	/**
	 * 内部的に使用する単純なフィールドの結合
	 */
	static class Combine extends Condition {
		
		private ComparisionOp op;
		private Select col1;
		private Select col2;
		
		public Combine(Select col1, Select col2) {
			this(col1, col2, ComparisionOp.Equal);
		}
		
		public Combine(Select col1, Select col2, ComparisionOp op) {
			this.col1 = col1;
			this.col2 = col2;
			this.op = op;
		}
		
		public ComparisionOp getOp() { return this.op;}
		public Select getCol1() { return this.col1;}
		public Select getCol2() { return this.col2;}
		
		public void build(StringBuilder buf, SelectBuilder builder) {
			col1.build(buf, builder);
			buf.append(" ").append(op).append(" ");
			col2.build(buf, builder);
		}
		
	}
	
	/**
	 * 比較演算子を使用する条件
	 */
	public abstract static class CompareParam extends Condition {
		
		protected Select col;
		protected ComparisionOp op;
		protected Object value;
		
		/**
		 * 左辺に使用するテーブルとフィールドと比較演算子を指定するコンストラクタ
		 */
		public CompareParam(Selectable table, String field, ComparisionOp op) {
			this.col = new Column(table, field);
			this.op = op;
		}
		
		/**
		 * 左辺にと比較演算子を指定するコンストラクタ
		 */
		public CompareParam(Select col, ComparisionOp op) {
			this.col = col;
			this.op = op;
		}
		
		/**
		 * 右辺に設定する値<br>
		 * 値が指定されない場合は右辺は「?」となります
		 */
		public CompareParam value(Object value) {
			this.value = value;
			return this;
		}
		
		/**
		 * 右辺に設定する列名<br>
		 * value(new Column(table, column))と同じです。
		 */
		public CompareParam column(Table table, String column) {
			return value(new Column(table, column));
		}
		
		/**
		 * 右辺に設定する列名<br>
		 * value(new Column(null, column))と同じです。
		 */
		public CompareParam column(String column) {
			return value(new Column(null, column));
		}
		
		public void build(StringBuilder buf, SelectBuilder builder) {
			this.col.build(buf, builder);
			buf.append(" ").append(this.op).append(" ");
			if (value == null) {
				buf.append("?");
			} else {
				SelectBuilder.appendObject(buf, builder, value);
			}
		}
		
	}
	
	/**
	 * 条件 xxx = ?
	 */
	public static class Equal extends CompareParam {
		
		public Equal(String field) {
			this(null, field);
		}
		
		public Equal(Selectable table, String field) {
			super(table, field, ComparisionOp.Equal);
		}
		
		public Equal(Select col) {
			super(col, ComparisionOp.Equal);
		}
	}
	
	/**
	 * 条件 xxx < ?
	 */
	public static class LessThan extends CompareParam {
		
		public LessThan(String field) {
			this(null, field);
		}
		
		public LessThan(Selectable table, String field) {
			super(table, field, ComparisionOp.LessThan);
		}
		
		public LessThan(Select col) {
			super(col, ComparisionOp.LessThan);
		}
	}
	
	/**
	 * 条件 xxx <= ?
	 */
	public static class LessEqual extends CompareParam {
		
		public LessEqual(String field) {
			this(null, field);
		}
		
		public LessEqual(Selectable table, String field) {
			super(table, field, ComparisionOp.LessEqual);
		}
		
		public LessEqual(Select col) {
			super(col, ComparisionOp.LessEqual);
		}
	}
	
	/**
	 * 条件 xxx > ?
	 */
	public static class GreaterThan extends CompareParam {
		
		public GreaterThan(String field) {
			this(null, field);
		}
		
		public GreaterThan(Selectable table, String field) {
			super(table, field, ComparisionOp.GreaterThan);
		}
		
		public GreaterThan(Select col) {
			super(col, ComparisionOp.GreaterThan);
		}
	}
	
	/**
	 * 条件 xxx >= ?
	 */
	public static class GreaterEqual extends CompareParam {
		
		public GreaterEqual(String field) {
			this(null, field);
		}
		
		public GreaterEqual(Selectable table, String field) {
			super(table, field, ComparisionOp.GreaterEqual);
		}
		
		public GreaterEqual(Select col) {
			super(col, ComparisionOp.GreaterEqual);
		}
	}
	
	/**
	 * 条件 xxx <> ?
	 */
	public static class NotEqual extends CompareParam {
		
		public NotEqual(String field) {
			this(null, field);
		}
		
		public NotEqual(Selectable table, String field) {
			super(table, field, ComparisionOp.NotEqual);
		}
		
		public NotEqual(Select col) {
			super(col, ComparisionOp.NotEqual);
		}
	}
	
	/**
	 * 条件 xxx LIKE ?
	 */
	public static class Like extends CompareParam {
		
		public Like(String field) {
			this(null, field);
		}
		
		public Like(Selectable table, String field) {
			super(table, field, ComparisionOp.Like);
		}
		
		public Like(Select col) {
			super(col, ComparisionOp.Like);
		}
	}
	
	/**
	 * 条件 xxx is null
	 */
	public static class IsNull extends CompareParam {
		
		public IsNull(String field) {
			this(null, field);
		}
		
		public IsNull(Selectable table, String field) {
			super(table, field, ComparisionOp.IsNull);
		}
		
		public IsNull(Select col) {
			super(col, ComparisionOp.IsNull);
		}
		
		@Override
		public void build(StringBuilder buf, SelectBuilder builder) {
			this.col.build(buf, builder);
			buf.append(" ").append(this.op).append(" ");
		}
	}
	
	/**
	 * 条件 xxx is not null
	 */
	public static class IsNotNull extends CompareParam {
		
		public IsNotNull(String field) {
			this(null, field);
		}
		
		public IsNotNull(Selectable table, String field) {
			super(table, field, ComparisionOp.IsNotNull);
		}
		
		public IsNotNull(Select col) {
			super(col, ComparisionOp.IsNotNull);
		}
		
		@Override
		public void build(StringBuilder buf, SelectBuilder builder) {
			this.col.build(buf, builder);
			buf.append(" ").append(this.op).append(" ");
		}
	}
	
	/**
	 * 条件 xxx IN (?,...)
	 */
	public static class In extends CompareParam {
		
		private int cnt;
		
		/**
		 * In(field, 1)と同じです
		 */
		public In(String field) {
			this(null, field, 1);
		}
		
		/**
		 * フィールド名とパラメータ数を指定するコンストラクタ
		 */
		public In(String field, int cnt) {
			this(null, field, cnt);
		}
		
		/**
		 * In(table, field, 1)と同じです
		 */
		public In(Selectable table, String field) {
			this(table, field, 1);
		}
		
		/**
		 * テーブルとフィールド名とパラメータ数を指定するコンストラクタ
		 */
		public In(Selectable table, String field, int cnt) {
			super(table, field, ComparisionOp.In);
			this.cnt = cnt;
		}
		
		
		public In(Select col) {
			this(col, 1);
		}
		
		
		public In(Select col, int cnt) {
			super(col, ComparisionOp.In);
			this.cnt = cnt;
		}
		
		/**
		 * 右辺に複数の値を設定します
		 */
		public In values(Object... values) {
			this.value = Arrays.asList(values);
			return this;
		}
		
		@Override
		public void build(StringBuilder buf, SelectBuilder builder) {
			List values = null;
			if (this.value != null) {
				if (this.value instanceof List) {
					values = (List)this.value;
				} else {
					values = Arrays.asList(value);
				}
			}
			this.col.build(buf, builder);
			buf.append(" ").append(this.op).append(" (");
			if (values == null) {
				for (int i=0; i<this.cnt; i++) {
					if (i != 0) {
						buf.append(", ");
					}
					buf.append("?");
				}
			} else {
				boolean first = true;
				for (Object o : values) {
					if (first) {
						first = false;
					} else {
						buf.append(", ");
					}
					SelectBuilder.appendObject(buf, builder, o);
				}
			}
			buf.append(")");
		}
	}
	
	/**
	 * 条件 EXISTS (...)
	 */
	public static class Exists extends Condition {
		
		private SelectBuilder builder;
		private boolean notExists = false;
		
		/**
		 * テーブルを指定してExistsを構築します
		 */
		public Exists(Selectable table) {
			builder = new SelectBuilder(table);
			builder.setAliasPrefix("E");
			builder.select(new Literal("*"));
		}
		
		/**
		 * SelectBuilderを指定してExistsを構築します
		 */
		public Exists(SelectBuilder builder) {
			this.builder = builder;
			builder.setAliasPrefix("E");
		}
		
		protected Exists(Selectable table, boolean notExists) {
			this(table);
			this.notExists = notExists;
		}
		
		protected Exists(SelectBuilder builder, boolean notExists) {
			this(builder);
			this.notExists = notExists;
		}
		
		/**
		 * 指定のフィールド名でFROMのテーブルと結合します。<br>
		 * フィールド名はEXISTS節のテーブルと親クエリのテーブルの
		 * 両方に含まれている必要があります。<br>
		 * @param field フィールド名
		 */
		public Exists join(String field) {
			return join(field, field);
		}
		
		/**
		 * 指定のフィールド名でFROMのテーブルと結合します。<br>
		 * @param field1 EXISTS句のテーブルのフィールド名
		 * @param field2 親クエリのテーブルのフィールド名
		 */
		public Exists join(String field1, String field2) {
			Selectable main = builder.getMainTable();
			if (!main.hasField(field1)) {
				if (main.hasField(field2)) {
					return join(field2, field1);
				}
				throw new IllegalArgumentException("Both " + field1 + " and " + field2 + " are not field of " + main); 
			}
			return join(main, field1, null, field2);
		}
		
		/**
		 * 指定のフィールド名でFROMのテーブルと結合します。<br>
		 * @param table1 EXISTS句のテーブル
		 * @param field1 EXISTS句のテーブルのフィールド名
		 * @param table2 親クエリのテーブル
		 * @param field3 親クエリのテーブルのフィールド名
		 */
		public Exists join(Selectable table1, String field1, Selectable table2, String field2) {
			if (table1 != builder.getMainTable()) {
				if (table2 != builder.getMainTable()) {
					throw new IllegalArgumentException("Invalid tables: " + table1 + ", " + table2);
				}
				return join(table2, field2, table1, field1);
			}
			Column col1 = new Column(builder.getMainTable(), field1);
			Column col2 = new Column(table2, field2);
			return where(new Combine(col1, col2));
		}
		
		/**
		 * AND条件でEXISTS句にWHERE条件を追加します。
		 */
		public Exists where(Condition cond) {
			builder.where(cond);
			return this;
		}
		
		/**
		 * AND条件でEXISTS句にWHERE条件を追加します。
		 */
		public Exists and(Condition cond) {
			builder.and(cond);
			return this;
		}
		
		/**
		 * OR条件でEXISTS句にWHERE条件を追加します。
		 */
		public Exists or(Condition cond) {
			builder.or(cond);
			return this;
		}
		
		@Override
		public void build(StringBuilder buf, SelectBuilder parent) {
			this.builder.setParent(parent);
			for (Condition c : this.builder.getConditions()) {
				if (c instanceof Combine) {
					Combine combi = (Combine)c;
					Column col2 = (Column)combi.getCol2();
					if (col2.getTable() == null) {
						col2.setTable(parent.searchTable(col2.getField()));
					}
				}
			}
			if (notExists) {
				buf.append("NOT ");
			}
			buf.append("EXISTS (")
				.append(this.builder.toSQL())
				.append(")");
		}
	}
	
	/**
	 * 条件 NOT EXISTS (...)
	 */
	public static class NotExists extends Exists {
		
		public NotExists(Selectable table) {
			super(table, true);
		}
		
		public NotExists(SelectBuilder builder) {
			super(builder, true);
		}
	}
	
}
