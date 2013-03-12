package jp.co.flect.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;

public class TableSourceGenerator {
	
	private Connection con;
	private File outputDir;
	private VelocityEngine engine;
	private String packageName;
	private String superClass = "Table";
	
	public TableSourceGenerator(Connection con, File outputDir) {
		this.con = con;
		this.outputDir = outputDir;
		
		this.engine = new VelocityEngine();
		this.engine.addProperty("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogSystem");
		this.engine.addProperty("resource.loader", "class");
		this.engine.addProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
		this.engine.init();
	}
	
	public String getPackageName() { return this.packageName;}
	public void setPackageName(String v) { this.packageName = v;}
	
	public String getSuperClass() { return this.superClass;}
	public void setSuperClass(String v) { this.superClass = v;}
	
	public void generate(String tableName) throws SQLException, IOException {
		String schemaName = null;
		int idx = tableName.indexOf('.');
		if (idx != -1) {
			schemaName = tableName.substring(0, idx);
			tableName = tableName.substring(idx + 1);
		}
		DatabaseMetaData meta = con.getMetaData();
		Set<String> keySet = new HashSet<String>();
		boolean useSerial = false;
		ResultSet rs = meta.getPrimaryKeys(null, schemaName, tableName);
		try {
			while (rs.next()) {
				String name = rs.getString(4);
				keySet.add(name);
			}
		} finally {
			rs.close();
		}
		
		TableInfo table = new TableInfo(tableName);
		rs = meta.getColumns(null, schemaName, tableName, "%");
		try {
			while (rs.next()) {
				String name = rs.getString(4);
				int type = rs.getInt(5);
				boolean pk = keySet.contains(name);
				boolean autoInc = rs.getBoolean(23);
				
				ColumnInfo col = new ColumnInfo(name, type, pk);
				table.addColumn(col);
				if (pk && keySet.size() == 1 && autoInc) {
					useSerial = true;
				}
			}
		} finally {
			rs.close();
		}
		
		File outputFile = new File(this.outputDir, table.getClazzName() + ".java");
		Writer writer = new OutputStreamWriter(new FileOutputStream(outputFile), "utf-8");
		try {
			Template template = engine.getTemplate("jp/co/flect/sql/Table.template");
			VelocityContext context = new VelocityContext();
			context.put("packageName", this.packageName);
			context.put("superClazz", this.superClass);
			context.put("table", table);
			template.merge(context, writer);
		} finally {
			writer.close();
		}
	}
	
	private static String toPascalCase(String str) {
		if (str == null || str.length() == 0) {
			return null;
		}
		str = str.toLowerCase();
		StringBuilder buf = new StringBuilder();
		boolean bUp = true;
		for (int i=0; i<str.length(); i++) {
			char c = str.charAt(i);
			if (c == '_') {
				bUp = true;
			} else if (bUp) {
				buf.append(Character.toUpperCase(c));
				bUp = false;
			} else {
				buf.append(c);
			}
		}
		return buf.toString();
	}
	
	public static class TableInfo {
		
		private String name;
		private boolean useSerialKey;
		private List<ColumnInfo> colList = new ArrayList<ColumnInfo>();
		
		public TableInfo(String name) {
			this.name = name;
		}
		
		public String getName() { return this.name;}
		
		public String getClazzName() {
			return toPascalCase(this.name);
		}
		
		public boolean useSerialKey() {
			return this.useSerialKey;
		}
		
		public void setUseSerialKey(boolean b) {
			this.useSerialKey = b;
		}
		
		public List<ColumnInfo> getColList() { return this.colList;}
		public void addColumn(ColumnInfo col) { this.colList.add(col);}
	}
	
	public static class ColumnInfo {
		
		private String name;
		private int type;
		private boolean primaryKey;
		
		public ColumnInfo(String name, int type, boolean pk) {
			this.name = name;
			this.type = type;
			this.primaryKey = pk;
		}
		
		public String getName() { return this.name;}
		public int getType() { return this.type;}
		public boolean isPrimaryKey() { return this.primaryKey;}
		
		public String getDeclareName() { return this.name.toUpperCase();}
		public String getTypeString() {
			try {
				for (Field f : Types.class.getFields()) {
					int mod = f.getModifiers();
					if (Modifier.isStatic(mod) && Modifier.isFinal(mod) && f.getType().equals(int.class)) {
						if (f.getInt(null) == this.type) {
							return f.getName();
						}
					}
				}
			} catch (IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
			throw new IllegalStateException();
		}
		
		public String getNamePascalCase() { return toPascalCase(this.name);}
		
		public String getJavaType() {
			switch (this.type) {
				case Types.BIGINT:
				case Types.ROWID:
					return "long";
				case Types.BLOB:
				case Types.BINARY:
					return "byte[]";
				case Types.BIT:
				case Types.BOOLEAN:
					return "boolean";
				case Types.CHAR:
				case Types.CLOB:
				case Types.NCHAR:
				case Types.NCLOB:
				case Types.NVARCHAR:
				case Types.SQLXML:
				case Types.VARCHAR:
					return "String";
				case Types.DATE:
					return "Date";
				case Types.DECIMAL:
				case Types.NUMERIC:
					return "BigDecimal";
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.REAL:
					return "dobule";
				case Types.INTEGER:
				case Types.SMALLINT:
				case Types.TINYINT:
					return "int";
				case Types.TIME:
				case Types.TIMESTAMP:
					return "Date";
				case Types.OTHER:
				case Types.REF:
				case Types.STRUCT:
				case Types.VARBINARY:
				case Types.ARRAY:
				case Types.DATALINK:
				case Types.DISTINCT:
				case Types.JAVA_OBJECT:
				case Types.LONGNVARCHAR:
				case Types.LONGVARBINARY:
				case Types.LONGVARCHAR:
				case Types.NULL:
				default:
					throw new IllegalArgumentException("UnsupportedType: " + type);
			}
		}
		
		public String getJavaTypePascalCase() {
			String ret = getJavaType();
			if (ret.endsWith("[]")) {
				ret = ret.substring(0, ret.length() - 2) + "s";
			}
			char c = ret.charAt(0);
			if (Character.isUpperCase(c)) {
				return ret;
			} else {
				return Character.toUpperCase(c) + ret.substring(1);
			}
		}
	}
}
