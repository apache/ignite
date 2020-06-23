package de.bwaldvogel.mongo.backend.postgresql;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.MongoServerException;

final class JsonConverter {

	private static final ObjectMapper objectMapper = objectMapper();

	private JsonConverter() {
	}

	public static class DocumentDeserializationHandder extends DeserializationProblemHandler{
		
		 public JavaType handleMissingTypeId(DeserializationContext ctxt,
		            JavaType baseType, TypeIdResolver idResolver,
		            String failureMsg)
		        throws IOException
		    {
			 	return ctxt.constructType(Document.class);
		        
		    }
		 
		 public JavaType handleUnknownTypeId(DeserializationContext ctxt,
		            JavaType baseType, String subTypeId, TypeIdResolver idResolver,
		            String failureMsg)
		        throws IOException
		    {
			 	return ctxt.constructType(Document.class);
		    }
	}
	public static class DocumentDeserializer extends StdDeserializer<Document> {

		public DocumentDeserializer() {
			this(null);
		}

		public DocumentDeserializer(Class<?> vc) {
			super(vc);
		}

		@Override
		public Document deserialize(JsonParser jp, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {

			JsonNode productNode = jp.getCodec().readTree(jp);
			Document product = new Document();

			if (!productNode.isObject()) {
				return product;
			}
			Iterator<Entry<String, JsonNode>> it = productNode.fields();

			while (it.hasNext()) {
				Map.Entry<String, JsonNode> node = it.next();
				
				product.append(node.getKey(), deserialize(node.getValue(), ctxt));
				
			}

			return product;
		}

		// 需要排重
		public Object deserialize(JsonNode productNode, DeserializationContext ctxt) {
			if (!productNode.isObject()) {
				if(productNode.isBigInteger() || productNode.isLong() || productNode.isInt() || productNode.isShort())
					return productNode.asLong();
				if(productNode.isNumber())
					return productNode.asDouble();
				if(productNode.isBoolean()) {
					return productNode.asBoolean();
				}
				if(productNode.isTextual()) {
					return productNode.asText();
				}
				if(productNode.isArray()) {
					return productNode;
				}
				return productNode;
			}
			
			//objectMapper.canDeserialize(productNode.)
			//ctxt.findRootValueDeserializer(type)
			//ctxt.findContextualValueDeserializer(productNode.getNodeType(), null);
			
			Document product = new Document();
			Iterator<Entry<String, JsonNode>> it = productNode.fields();

			while (it.hasNext()) {
				Map.Entry<String, JsonNode> node = it.next();
				if (node.getValue().isObject()) {
					Object doc = deserialize(node.getValue(), ctxt);
					product.append(node.getKey(), doc);
				} else {
					product.append(node.getKey(), node.getValue());
				}
			}

			return product;
		}
	}

	private static ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

		objectMapper.activateDefaultTyping(objectMapper.getPolymorphicTypeValidator(), DefaultTyping.JAVA_LANG_OBJECT,
				JsonTypeInfo.As.PROPERTY);
		

		objectMapper.registerSubtypes(ObjectId.class);
		objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
		objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

		SimpleModule module = new SimpleModule();
		module.addAbstractTypeMapping(Set.class, LinkedHashSet.class);
		module.addAbstractTypeMapping(Map.class, LinkedHashMap.class);
		//module.addDeserializer(Document.class, new DocumentDeserializer());
		objectMapper.addHandler(new DocumentDeserializationHandder());

		objectMapper.registerModule(module);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		objectMapper.configure(SerializationFeature.WRITE_DATES_WITH_ZONE_ID, true);
		objectMapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, true);
		return objectMapper;
	}

	static String toJson(Object object) {
		ObjectWriter writer = objectMapper.writer();
		try {
			return writer.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new MongoServerException("Failed to serialize value to JSON", e);
		}
	}

	static Document fromJson(String json) throws IOException {
		ObjectReader reader = objectMapper.reader();
		return reader.forType(Document.class).readValue(json);
	}

}
