package be.nabu.eai.module.jdbc.dialects.h2;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.libs.evaluator.QueryParser;
import be.nabu.libs.evaluator.QueryPart;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.base.Duration;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.GeneratedProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.UniqueProperty;

public class H2Dialect implements SQLDialect {

	public static void main(String...args) throws ParseException {
		System.out.println(rewriteMerge("insert into ~nodes (\n" + 
				"	id,\n" + 
				"	created,\n" + 
				"	modified,\n" + 
				"	started,\n" + 
				"	stopped,\n" + 
				"	owner_id,\n" + 
				"	verified,\n" + 
				"	enabled,\n" + 
				"	parent_id,\n" + 
				"	version,\n" + 
				"	priority,\n" + 
				"	name,\n" + 
				"	title,\n" + 
				"	description,\n" + 
				"	path,\n" + 
				"	slug,\n" + 
				"	language_id,\n" + 
				"	component_id\n" + 
				") values (\n" + 
				"	:id,\n" + 
				"	:created,\n" + 
				"	:modified,\n" + 
				"	:started,\n" + 
				"	:stopped,\n" + 
				"	:ownerId,\n" + 
				"	:verified,\n" + 
				"	:enabled,\n" + 
				"	:parentId,\n" + 
				"	:version,\n" + 
				"	:priority,\n" + 
				"	:name,\n" + 
				"	:title,\n" + 
				"	:description,\n" + 
				"	:path,\n" + 
				"	:slug,\n" + 
				"	:languageId,\n" + 
				"	:componentId\n" + 
				")\n" + 
				"on conflict(id) do update set\n" + 
				"	id = excluded.id,\n" + 
				"	created = excluded.created,\n" + 
				"	modified = excluded.modified,\n" + 
				"	started = excluded.started,\n" + 
				"	stopped = excluded.stopped,\n" + 
				"	owner_id = excluded.owner_id,\n" + 
				"	verified = excluded.verified,\n" + 
				"	enabled = excluded.enabled,\n" + 
				"	parent_id = excluded.parent_id,\n" + 
				"	version = excluded.version,\n" + 
				"	priority = excluded.priority,\n" + 
				"	name = excluded.name,\n" + 
				"	title = excluded.title,\n" + 
				"	description = excluded.description,\n" + 
				"	path = excluded.path,\n" + 
				"	slug = excluded.slug,\n" + 
				"	language_id = excluded.language_id,\n" + 
				"	component_id = excluded.component_id"));
	}
	
	@Override
	public String getSQLName(Class<?> instanceClass) {
		if (UUID.class.isAssignableFrom(instanceClass)) {
			return "uuid";
		}
		else {
			return SQLDialect.super.getSQLName(instanceClass);
		}
	}
	
	@Override
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		// we have a merge statement
		if (sql.matches("(?i)(?s)[\\s]*\\binsert into\\b.*\\bon conflict\\b.*\\bdo update\\b.*")) {
			try {
				sql = rewriteMerge(sql);
			}
			catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		return sql;
	}
	
	private static boolean validate(List<QueryPart> tokens, int offset, String value) {
		return tokens.get(offset).getToken().getContent().toLowerCase().equals(value.toLowerCase());
	}
	
	public static String rewriteMerge(String sql) throws ParseException {
		List<QueryPart> parsed = QueryParser.getInstance().interpret(QueryParser.getInstance().tokenize(sql), true);
		int counter = 0;
		if (!validate(parsed, counter++, "insert") || !validate(parsed, counter++, "into")) {
			throw new ParseException("Expecting 'insert into'", counter);
		}
		
		String table = parsed.get(counter++).getToken().getContent();
		if (table.equals("~")) {
			table += parsed.get(counter++).getToken().getContent();
		}
		System.out.println("Table: " + table);
		if (!validate(parsed, counter++, "(")) {
			throw new ParseException("Expecting opening '(' to list the fields", counter);
		}
		List<String> fields = new ArrayList<String>();
		while (counter < parsed.size()) {
			if (validate(parsed, counter, ")")) {
				counter++;
				break;
			}
			// need a separator for more fields
			else if (!fields.isEmpty() && !validate(parsed, counter++, ",")) {
				throw new ParseException("Expecting either a ',' to separate the fields or a ')' to stop them", counter);
			}
			fields.add(parsed.get(counter++).getToken().getContent());
		}
		System.out.println("Fields: " + fields);
		
		if (!validate(parsed, counter++, "values")) {
			throw new ParseException("Expecting fixed string 'values' indicating start of values", counter);
		}
		
		List<List<String>> values = new ArrayList<List<String>>();
		List<String> current = null;
		boolean isNamed = false;
		while (counter < parsed.size()) {
			// we start a new value sequence
			if (validate(parsed, counter, "(")) {
				if (current != null) {
					throw new ParseException("List of values not closed properly", counter);
				}
				counter++;
				current = new ArrayList<String>();
				values.add(current);
			}
			else if (validate(parsed, counter, ")")) {
				counter++;
				current = null;
			}
			else if (validate(parsed, counter, ",")) {
				if (values.isEmpty()) {
					throw new ParseException("Unexpected value list separator", counter);
				}
				counter++;
			}
			else if (validate(parsed, counter, ":")) {
				isNamed = true;
				counter++;
			}
			// if we don't have a current value list and we are encountering other tokens, we have exited the value listing
			else if (current == null) {
				break;
			}
			else {
				if (isNamed) {
					current.add(":" + parsed.get(counter++).getToken().getContent());
					isNamed = false;
				}
				else {
					current.add(parsed.get(counter++).getToken().getContent());
				}
			}
		}
		System.out.println("Values: " + values);
		
		if (!validate(parsed, counter++, "on") || !validate(parsed, counter++, "conflict")) {
			throw new ParseException("Expecting 'on conflict'", counter);
		}
		if (!validate(parsed, counter++, "(")) {
			throw new ParseException("Expecting brackets around the conflicted fields", counter);
		}
		List<String> conflicts = new ArrayList<String>();
		while (counter < parsed.size()) {
			if (validate(parsed, counter, ")")) {
				counter++;
				break;
			}
			// need a separator for more fields
			else if (!conflicts.isEmpty() && !validate(parsed, counter++, ",")) {
				throw new ParseException("Expecting either a ',' to separate the conflicted fields or a ')' to stop them", counter);
			}
			String conflict = parsed.get(counter++).getToken().getContent();
			if (!fields.contains(conflict)) {
				throw new ParseException("The conflicted field '" + conflict + "' is not in the field list that is inserted", counter);
			}
			conflicts.add(conflict);
		}
		System.out.println("Conflicts: " + conflicts);
		
		if (!validate(parsed, counter++, "do") || !validate(parsed, counter++, "update") || !validate(parsed, counter++, "set")) {
			throw new ParseException("Expecting 'do update set'", counter);
		}
		
		StringBuilder result = new StringBuilder();
		result.append("merge into ")
			.append(table)
			.append(" (");
		
		boolean first = true;
		for (String field : fields) {
			if (first) {
				first = false;
			}
			else {
				result.append(",");
			}
			result.append("\n\t").append(field);
		}
		result.append("\n) key(");
		first = true;
		for (String field : conflicts) {
			if (first) {
				first = false;
			}
			else {
				result.append(",");
			}
			result.append("\n\t").append(field);
		}
		result.append("\n)");
		
		for (List<String> value : values) {
			result.append(" values (");
			first = true;
			for (String field : value) {
				if (first) {
					first = false;
				}
				else {
					result.append(",");
				}
				result.append("\n\t").append(field);
			} 
			result.append("\n)");
		}
		
		return result.toString();
	}

	@Override
	public String limit(String sql, Long offset, Integer limit) {
		if (offset != null) {
			sql = sql + " OFFSET " + offset;
		}
		if (limit != null) {
			sql = sql + " LIMIT " + limit;
		}
		return sql;
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}

	@Override
	public String buildCreateSQL(ComplexType type, boolean compact) {
		StringBuilder builder = new StringBuilder();
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			Value<Boolean> generatedProperty = child.getProperty(GeneratedProperty.getInstance());
			if (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue()) {
				String seqName = "seq_" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + "_" + EAIRepositoryUtils.uncamelify(child.getName()); 
				builder.append("create sequence ").append(seqName).append(";\n");
			}
		}
		builder.append("create table " + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " (\n");
		boolean first = true;
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			if (first) {
				first = false;
			}
			else {
				builder.append(",\n");
			}
			
			// if we have a complex type, generate an id field that references it
			if (child.getType() instanceof ComplexType) {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + "_id uuid");
			}
			// differentiate between dates
			else if (Date.class.isAssignableFrom(((SimpleType<?>) child.getType()).getInstanceClass())) {
				Value<String> property = child.getProperty(FormatProperty.getInstance());
				String format = property == null ? "dateTime" : property.getValue();
				if (format.equals("dateTime")) {
					format = "timestamp";
				}
				else if (!format.equals("date") && !format.equals("time")) {
					format = "timestamp";
				}
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ").append(format);
			}
			else {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ")
					.append(getPredefinedSQLType(((SimpleType<?>) child.getType()).getInstanceClass()));
			}
			
			Value<Boolean> generatedProperty = child.getProperty(GeneratedProperty.getInstance());
			if (child.getName().equals("id")) {
				builder.append(" primary key");
			}
			else {
				Integer value = ValueUtils.getValue(MinOccursProperty.getInstance(), child.getProperties());
				if (value == null || value > 0 || (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue())) {
					builder.append(" not null");
				}
			}
			
			Value<Boolean> property = child.getProperty(UniqueProperty.getInstance());
			if (property != null && property.getValue()) {
				builder.append(" unique");
			}
			
			if (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue()) {
				String seqName = "seq_" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + "_" + EAIRepositoryUtils.uncamelify(child.getName());
				builder.append(" default " + seqName + ".nextval");
			}
		}
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			Value<String> foreignKey = child.getProperty(ForeignKeyProperty.getInstance());
			if (foreignKey != null) {
				String[] split = foreignKey.getValue().split(":");
				if (split.length == 2) {
					DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(split[0]);
					String referencedName = ValueUtils.getValue(CollectionNameProperty.getInstance(), resolve.getProperties());
					if (referencedName == null) {
						referencedName = resolve.getName();
					}
					builder.append(",\n\tforeign key (" + EAIRepositoryUtils.uncamelify(child.getName()) + ") references " + EAIRepositoryUtils.uncamelify(referencedName) + "(" + split[1] + ")");
				}
			}
		}
		
		builder.append("\n);");
		return builder.toString();
	}

	public static String getPredefinedSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
			// best practice to use application level limits on text
			return "varchar";
		}
		else if (Duration.class.isAssignableFrom(instanceClass)) {
			return "interval";
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return "binary";
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return "int";
		}
		else if (Long.class.isAssignableFrom(instanceClass) || BigInteger.class.isAssignableFrom(instanceClass)) {
			return "bigint";
		}
		else if (Double.class.isAssignableFrom(instanceClass) || BigDecimal.class.isAssignableFrom(instanceClass)) {
			return "decimal";
		}
		else if (Float.class.isAssignableFrom(instanceClass)) {
			return "decimal";
		}
		else if (Short.class.isAssignableFrom(instanceClass)) {
			return "smallint";
		}
		else if (Boolean.class.isAssignableFrom(instanceClass)) {
			return "boolean";
		}
		else if (UUID.class.isAssignableFrom(instanceClass)) {
			return "uuid";
		}
		else if (Date.class.isAssignableFrom(instanceClass)) {
			return "timestamp";
		}
		else {
			return null;
		}
	}
	
	@Override
	public String buildInsertSQL(ComplexContent values, boolean compact) {
		// TODO Auto-generated method stub
		return null;
	}

}
