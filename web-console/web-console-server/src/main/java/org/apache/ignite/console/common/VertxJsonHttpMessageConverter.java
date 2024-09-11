package org.apache.ignite.console.common;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.json.AbstractJsonHttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class VertxJsonHttpMessageConverter extends AbstractJsonHttpMessageConverter{
	
	protected ObjectMapper defaultObjectMapper;
	

	@Nullable
	private Boolean prettyPrint;
	
	/**
	 * Configure the main {@code ObjectMapper} to use for Object conversion.
	 * If not set, a default {@link ObjectMapper} instance is created.
	 * <p>Setting a custom-configured {@code ObjectMapper} is one way to take
	 * further control of the JSON serialization process. For example, an extended
	 * {@link com.fasterxml.jackson.databind.ser.SerializerFactory}
	 * can be configured that provides custom serializers for specific types.
	 * Another option for refining the serialization process is to use Jackson's
	 * provided annotations on the types to be serialized, in which case a
	 * custom-configured ObjectMapper is unnecessary.
	 * @see #registerObjectMappersForType(Class, Consumer)
	 */
	public void setObjectMapper(ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "ObjectMapper must not be null");
		this.defaultObjectMapper = objectMapper;
		configurePrettyPrint();
	}

	/**
	 * Return the main {@code ObjectMapper} in use.
	 */
	public ObjectMapper getObjectMapper() {
		return this.defaultObjectMapper;
	}
	
	/**
	 * Whether to use the {@link DefaultPrettyPrinter} when writing JSON.
	 * This is a shortcut for setting up an {@code ObjectMapper} as follows:
	 * <pre class="code">
	 * ObjectMapper mapper = new ObjectMapper();
	 * mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
	 * converter.setObjectMapper(mapper);
	 * </pre>
	 */
	public void setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
		configurePrettyPrint();
	}

	
	private void configurePrettyPrint() {
		if (this.prettyPrint != null) {
			this.defaultObjectMapper.configure(SerializationFeature.INDENT_OUTPUT, this.prettyPrint);
		}
	}
	
	@Override
	protected boolean supports(Class<?> clazz) {
		return clazz==JsonObject.class || clazz==JsonArray.class;
	}

	@Override
	protected Object readInternal(Type resolvedType, Reader reader) throws Exception {
		if(resolvedType==JsonArray.class) {
			List<Object> data = defaultObjectMapper.reader().readValue(reader,List.class);			
			return new JsonArray(data);
		}
		else {
			Map<String,Object> data = defaultObjectMapper.reader().readValue(reader,Map.class);			
			return new JsonObject(data);
		}
	}

	@Override
	protected void writeInternal(Object object, Type type, Writer writer) throws Exception {
		writer.append(object.toString());		
	}
}
