package org.elasticsearch.relay.util;

import java.util.Arrays;
import java.util.Map;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SqlTemplateParse {
	
	 static class MapPropertyAccessor implements PropertyAccessor {
		 
        @Override
        public Class<?>[] getSpecificTargetClasses() {
            return new Class[]{Map.class};
        }

        @Override
        public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
            return (target instanceof Map && ((Map<?, ?>) target).containsKey(name));
        }

        @Override
        public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
            
            Map<?, ?> map = (Map<?, ?>) target;	            
            Object value = map.get(name);
            if(value==null) {
            	return TypedValue.NULL;
            }            
            return new TypedValue(value);
        }

        @Override
        public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
            return false;
        }

        @Override
        public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {

        }
    }
	 
	 static class ObjectNodePropertyAccessor implements PropertyAccessor {
		 
	        @Override
	        public Class<?>[] getSpecificTargetClasses() {
	            return new Class[]{ObjectNode.class};
	        }

	        @Override
	        public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
	            return (target instanceof ObjectNode && ((ObjectNode) target).get(name)!=null);
	        }

	        @Override
	        public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
	            
	        	ObjectNode map = (ObjectNode) target;	            
	            JsonNode value = map.get(name);
	            if(value==null) {
	            	return TypedValue.NULL;
	            }
	            return new TypedValue(renderString(value));
	        }

	        
	        @Override
	        public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
	            return false;
	        }

	        @Override
	        public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {

	        }
	    }
	 
	private static  SpelExpressionParser parser = new SpelExpressionParser();
	
	/**
	 * The default ParserContext implementation that enables template expression parsing
	 * mode. The expression prefix is #{ and the expression suffix is }.
	 * @see #isTemplate()
	 */
	static final ParserContext TEMPLATE_EXPRESSION = new ParserContext() {

		@Override
		public String getExpressionPrefix() {
			return "${";
		}

		@Override
		public String getExpressionSuffix() {
			return "}";
		}

		@Override
		public boolean isTemplate() {
			return true;
		}

	};
	
	private MapPropertyAccessor propertyAccessor = new MapPropertyAccessor();
	private ObjectNodePropertyAccessor jsonObjectPropertyAccessor = new ObjectNodePropertyAccessor();
	private  Expression expression;
	
	public SqlTemplateParse(String template ) {		
		this.expression = parser.parseExpression(template, TEMPLATE_EXPRESSION);
	}
		
	public Object getValue(Map<String,Object> model) throws Exception {
        StandardEvaluationContext context = new StandardEvaluationContext(model);
        context.addPropertyAccessor(propertyAccessor);
        return expression.getValue(context);
    }
	
	public Object getValue(ObjectNode model) throws Exception {
        StandardEvaluationContext context = new StandardEvaluationContext(model);
        context.addPropertyAccessor(jsonObjectPropertyAccessor);
        return expression.getValue(context);
    }
	
	public static String renderString(Object value) {
        if (value == null)
        	return "null";
        if( value instanceof JsonNode) {
        	return renderString((JsonNode)value);
        }
        if( value instanceof Number) {
        	return value.toString();
        }
        if( value instanceof CharSequence) {
        	return "'"+value.toString().replaceAll("\'","\'\'")+"'";
        }

        if(value.getClass().isArray() && value instanceof Object[]) {
        	Object[] a = (Object[])value;
        	int iMax = a.length - 1;
            if (iMax == -1)
                value = "()";
            else {
                StringBuilder b = new StringBuilder();
                b.append('(');
                for (int i = 0; ; i++) {
                	if(a[i]==null) continue;
                    b.append(renderString(a[i]));
                    if (i == iMax) {
                         b.append(')');
                         break;
                    }
                    b.append(", ");
                }
                value = b.toString();
            }            
        }
        return value.toString();
    }
	
	public static String renderString(JsonNode value) {
        if (value == null)
        	return "null";
        
        if( value.isNumber()) {
        	return value.toString();
        }
        if( value.isTextual()) {
        	return "'"+value.asText().replaceAll("\'","\'\'")+"'";
        }
        String output = null;
        if(value.isArray()) {
        	ArrayNode a = (ArrayNode)value;
        	int iMax = a.size() - 1;
            if (iMax == -1)
            	output = "()";
            else {
                StringBuilder b = new StringBuilder();
                b.append('(');
                for (int i = 0; ; i++) {
                	if(a.path(i)==null) continue;
                    b.append(renderString(a.path(i)));
                    if (i == iMax) {
                         b.append(')');
                         break;
                    }
                    b.append(", ");
                }
                output = b.toString();
            }            
        }
        return output;
    }
	
}
