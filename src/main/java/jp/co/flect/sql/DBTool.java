package jp.co.flect.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ParameterMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.reflect.Array;

import jp.co.flect.sql.Table.Field;

/**
 * 汎用のDatabaseユーティリティ
 */
public class DBTool {
	
	protected Connection con;
	
	public DBTool(Connection con) {
		this.con = con;
	}
	
	public Connection getConnection() { return this.con;}
	
	/**
	 * ResultSetからオブジェクトを生成するインターフェース
	 */
	public interface Creator<T> {
		public T create(ResultSet rs) throws SQLException;
	}
	
	/**
	 * バッチ処理を実行するインターフェース
	 */
	public interface Batch {
		public boolean addBatch(PreparedStatement stmt) throws SQLException;
	}
	
	public void setParameters(PreparedStatement stmt, Object... params) throws SQLException {
		ParameterMetaData metaData = null;
		int idx = 0;
		for (int i=0; i<params.length; i++) {
			idx++;
			Object o = params[i];
			if (o == null) {
				if (metaData == null) {
					metaData = stmt.getParameterMetaData();
				}
				int sqlType = metaData.getParameterType(idx);
				stmt.setNull(idx, sqlType);
			} else if (o.getClass().isArray()) {
				int len = Array.getLength(o);
				for (int j=0; j<len; j++) {
					if (j != 0) {
						idx++;
					}
					setParameter(stmt, idx, Array.get(o, j));
				}
			} else {
				setParameter(stmt, idx, o);
			}
		}
	}
	
	public void setParameter(PreparedStatement stmt, int idx, Object o) throws SQLException {
		if (o instanceof String) {
			stmt.setString(idx, o.toString());
		} else if (o instanceof Integer) {
			stmt.setInt(idx, ((Integer)o).intValue());
		} else if (o instanceof Boolean) {
			stmt.setBoolean(idx, ((Boolean)o).booleanValue());
		} else if (o instanceof Long) {
			stmt.setLong(idx, ((Long)o).longValue());
		} else if (o instanceof Double) {
			stmt.setDouble(idx, ((Double)o).doubleValue());
		} else if (o instanceof BigDecimal) {
			stmt.setBigDecimal(idx, (BigDecimal)o);
		} else if (o instanceof java.sql.Date) {
			stmt.setDate(idx, (java.sql.Date)o);
		} else if (o instanceof Timestamp) {
			stmt.setTimestamp(idx, (Timestamp)o);
		} else {
			throw new IllegalStateException(o.getClass().toString());
		}
	}
	
	private void setParameter(PreparedStatement stmt, int idx, Table t, Field f) throws SQLException {
		Object o = convertDate(f, t.get(f.getName()));
		if (o == null) {
			stmt.setNull(idx, f.getType());
		} else {
			setParameter(stmt, idx, o);
		}
	}
	
	private Object convertDate(Field f, Object o) {
		if (o == null) {
			return null;
		}
		switch (f.getType()) {
			case Types.DATE:
				if (!(o instanceof java.sql.Date)) {
					o = new Timestamp(((java.util.Date)o).getTime());
				}
				break;
			case Types.TIMESTAMP:
				if (!(o instanceof Timestamp)) {
					o = new Timestamp(((java.util.Date)o).getTime());
				}
				break;
		}
		return o;
	}
	
	/**
	 * SQLを実行してオブジェクトを生成する汎用メソッド
	 */
	public <T> T create(String sql, Creator<T> creator, Object... params) throws SQLException {
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			setParameters(stmt, params);
			ResultSet rs = stmt.executeQuery();
			try {
				return creator.create(rs);
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
	}
	
	/**
	 * 件数取得SQLを実行する汎用メソッド
	 */
	public int getCount(String sql, Object... params) throws SQLException {
		Creator<Integer> c = new Creator<Integer>() {
			public Integer create(ResultSet rs) throws SQLException {
				if (rs.next()) {
					return rs.getInt(1);
				}
				throw new IllegalStateException();
			}
		};
		return create(sql, c, params);
	}
	
	/**
	 * 1行1列の数値を取得するSQLを実行する汎用メソッド
	 */
	public Integer getInt(String sql, Object... params) throws SQLException {
		Creator<Integer> c = new Creator<Integer>() {
			public Integer create(ResultSet rs) throws SQLException {
				if (rs.next()) {
					int ret = rs.getInt(1);
					return rs.wasNull() ? null : ret;
				}
				return null;
			}
		};
		return create(sql, c, params);
	}
	
	/**
	 * 1行1列の文字列を取得するSQLを実行する汎用メソッド
	 */
	public String getString(String sql, Object... params) throws SQLException {
		Creator<String> c = new Creator<String>() {
			public String create(ResultSet rs) throws SQLException {
				if (rs.next()) {
					return rs.getString(1);
				}
				return null;
			}
		};
		return create(sql, c, params);
	}
	
	/**
	 * 1列の文字列のリストを取得するSQLを実行する汎用メソッド
	 */
	public List<String> getStringList(String sql, Object... params) throws SQLException {
		Creator<List<String>> c = new Creator<List<String>>() {
			public List<String> create(ResultSet rs) throws SQLException {
				ArrayList<String> list = new ArrayList<String>();
				while (rs.next()) {
					list.add(rs.getString(1));
				}
				return list.size() == 0 ? null : list;
			}
		};
		return create(sql, c, params);
	}
	
	/**
	 * 1列のintのリストを取得するSQLを実行する汎用メソッド
	 */
	public List<Integer> getIntList(String sql, Object... params) throws SQLException {
		Creator<List<Integer>> c = new Creator<List<Integer>>() {
			public List<Integer> create(ResultSet rs) throws SQLException {
				ArrayList<Integer> list = new ArrayList<Integer>();
				while (rs.next()) {
					list.add(rs.getInt(1));
				}
				return list.size() == 0 ? null : list;
			}
		};
		return create(sql, c, params);
	}
	
	/**
	 * 更新SQLを実行する汎用メソッド
	 */
	public int executeUpdate(String sql, Object... params) throws SQLException {
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			setParameters(stmt, params);
			return stmt.executeUpdate();
		} finally {
			stmt.close();
		}
	}
	
	/**
	 * 同一の更新SQLを繰り返し実行する汎用メソッド
	 */
	public int[] executeBatch(String sql, Batch batch) throws SQLException {
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			while (batch.addBatch(stmt)) {
				stmt.addBatch();
			}
			return stmt.executeBatch();
		} finally {
			stmt.close();
		}
	}
	
	/**
	 * 引数のTableに設定された値をINSERTします。<br>
	 * Tableの主キーがAutoNumberの場合は生成されたNumberが返ります。
	 */
	public int insert(Table t) throws SQLException {
		t.fireBeforeInsert();
		List<Field> fields = t.listValued();
		if (fields == null || fields.size() == 0) {
			throw new IllegalArgumentException();
		}
		String sql = t.buildInsertStatement();
		
		int rgk = t.useSerialKey() ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
		PreparedStatement stmt = con.prepareStatement(sql, rgk);
		try {
			for (int i=0; i<fields.size(); i++) {
				Field f = fields.get(i);
				setParameter(stmt, i+1, t, f);
			}
			stmt.executeUpdate();
			int ret = -1;
			if (t.useSerialKey()) {
				ResultSet rs = stmt.getGeneratedKeys();
				try {
					if (rs.next()) {
						ret = rs.getInt(1);
					}
				} finally {
					rs.close();
				}
			}
			t.fireAfterInsert();
			return ret;
		} finally {
			stmt.close();
		}
	}
	
	/**
	 * 単一テーブルの複数行をまとめてINSERTします。<br>
	 * Listの各行で設定されているフィールド名のセットはすべて同じでなければなりません。<br>
	 * 主キーがAutoNumberの場合、生成されたNumberは取得できません。
	 */
	public void insert(List<? extends Table> list) throws SQLException {
		int cv = -1;
		for (Table t : list) {
			t.fireBeforeInsert();
			if (cv == -1) {
				cv = t.countValued();
			} else if (cv != t.countValued()) {
				throw new IllegalArgumentException();
			}
		}
		List<Field> fields = list.get(0).listValued();
		if (fields == null || fields.size() == 0) {
			throw new IllegalArgumentException();
		}
		String sql = list.get(0).buildInsertStatement();
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			for (Table t : list) {
				for (int i=0; i<fields.size(); i++) {
					Field f = fields.get(i);
					setParameter(stmt, i+1, t, f);
				}
				stmt.addBatch();
			}
			stmt.executeBatch();
			for (Table t : list) {
				t.fireAfterInsert();
			}
		} finally {
			stmt.close();
		}
	} 
	
	/**
	 * 引数のTableに設定された値をUPDATEします。<br>
	 * WHERE句には主キーが使用されます。
	 */
	public int update(Table t) throws SQLException {
		return update(t, t.listKeys());
	}
	
	/**
	 * 引数のTableに設定された値をUPDATEします。<br>
	 * WHERE句にはwheresで指定されたFieldが使用されます。
	 */
	public int update(Table t, Field... wheres) throws SQLException {
		return update(t, Arrays.asList(wheres));
	}
	
	/**
	 * 引数のTableに設定された値をUPDATEします。<br>
	 * WHERE句にはwheresで指定されたFieldが使用されます。
	 */
	public int update(Table t, List<Field> wheres) throws SQLException {
		t.fireBeforeUpdate();
		List<Field> fields = t.listValued();
		if (fields == null || fields.size() == 0) {
			throw new IllegalArgumentException();
		}
		String sql = t.buildUpdateStatement(wheres);
		
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			int idx = 1;
			for (Field f : fields) {
				if (!wheres.contains(f)) {
					setParameter(stmt, idx++, t, f);
				}
			}
			for (Field f : wheres) {
				setParameter(stmt, idx++, t, f);
			}
			int ret = stmt.executeUpdate();
			t.fireAfterUpdate();
			return ret;
		} finally {
			stmt.close();
		}
	} 
	
	/**
	 * Listの先頭行でUPDATE文を生成しそれをすべてのTableに適用します。<br>
	 * WHERE句には主キーが使用されます。
	 */
	public int update(List<? extends Table> list) throws SQLException {
		return update(list, list.get(0).listKeys());
	}
	
	/**
	 * Listの先頭行でUPDATE文を生成しそれをすべてのTableに適用します。<br>
	 * WHERE句にはwheresで指定されたFieldが使用されます。
	 */
	public int update(List<? extends Table> list, Field... wheres) throws SQLException {
		return update(list, Arrays.asList(wheres));
	}
	
	/**
	 * Listの先頭行でUPDATE文を生成しそれをすべてのTableに適用します。<br>
	 * WHERE句にはwheresで指定されたFieldが使用されます。
	 */
	public int update(List<? extends Table> list, List<Field> wheres) throws SQLException {
		for (Table t : list) {
			t.fireBeforeUpdate();
		}
		
		List<Field> fields = list.get(0).listValued();
		if (fields == null || fields.size() == 0) {
			throw new IllegalArgumentException();
		}
		String sql = list.get(0).buildUpdateStatement(wheres);
		
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			for (Table t : list) {
				int idx = 1;
				for (Field f : fields) {
					if (!wheres.contains(f)) {
						setParameter(stmt, idx++, t, f);
					}
				}
				for (Field f : wheres) {
					setParameter(stmt, idx++, t, f);
				}
				stmt.addBatch();
			}
			int[] results = stmt.executeBatch();
			int ret = 0;
			for (int n : results) {
				ret += n;
			}
			for (Table t : list) {
				t.fireAfterUpdate();
			}
			return ret;
		} finally {
			stmt.close();
		}
	}
	
	public void commit() throws SQLException { con.commit();}
	
	//RollbackのExceptionは無視する
	public void rollback() { 
		try {
			con.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public int delete(Table t) throws SQLException {
		return delete(t, t.listKeys());
	}
	
	public int delete(Table t, Field... wheres) throws SQLException {
		return delete(t, Arrays.asList(wheres));
	}
	
	public int delete(Table t, List<Field> wheres) throws SQLException {
		t.fireBeforeDelete();
		String sql = t.buildDeleteStatement(wheres);
		
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			int idx = 1;
			for (Field f : wheres) {
				setParameter(stmt, idx++, t, f);
			}
			int ret = stmt.executeUpdate();
			t.fireAfterDelete();
			return ret;
		} finally {
			stmt.close();
		}
	}
	
	public int delete(List<? extends Table> list) throws SQLException {
		return delete(list, list.get(0).listKeys());
	}
	
	public int delete(List<? extends Table> list, Field... wheres) throws SQLException {
		return delete(list, Arrays.asList(wheres));
	}
	
	public int delete(List<? extends Table> list, List<Field> wheres) throws SQLException {
		for (Table t : list) {
			t.fireBeforeDelete();
		}
		String sql = list.get(0).buildDeleteStatement(wheres);
		
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			for (Table t : list) {
				int idx = 1;
				for (Field f : wheres) {
					setParameter(stmt, idx++, t, f);
				}
				stmt.addBatch();
			}
			int[] results = stmt.executeBatch();
			int ret = 0;
			for (int n : results) {
				ret += n;
			}
			for (Table t : list) {
				t.fireAfterDelete();
			}
			return ret;
		} finally {
			stmt.close();
		}
	}
	
}
