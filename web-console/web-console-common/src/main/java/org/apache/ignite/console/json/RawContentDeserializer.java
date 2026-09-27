

package org.apache.ignite.console.json;

import java.io.IOException;
import java.io.StringWriter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;

/**
 * Raw content deserializer that will deserialize any data as string.
 */
public class RawContentDeserializer extends JsonDeserializer<String> {
    /** */
    private final JsonFactory factory = new JsonFactory();

    /**
     * @param tok Token to process.
     * @param p Parser.
     * @param gen Generator.
     */
    private void writeToken(JsonToken tok, JsonParser p, JsonGenerator gen) throws IOException {
        switch (tok) {
            case FIELD_NAME:
                gen.writeFieldName(p.getText());
                break;

            case START_ARRAY:
                gen.writeStartArray();
                break;

            case END_ARRAY:
                gen.writeEndArray();
                break;

            case START_OBJECT:
                gen.writeStartObject();
                break;

            case END_OBJECT:
                gen.writeEndObject();
                break;

            case VALUE_NUMBER_INT:
                gen.writeNumber(p.getBigIntegerValue());
                break;

            case VALUE_NUMBER_FLOAT:
                gen.writeNumber(p.getDecimalValue());
                break;

            case VALUE_TRUE:
                gen.writeBoolean(true);
                break;

            case VALUE_FALSE:
                gen.writeBoolean(false);
                break;

            case VALUE_NULL:
                gen.writeNull();
                break;

            default:
                gen.writeString(p.getText());
        }
    }

    /** {@inheritDoc} */
    @Override public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonToken startTok = p.getCurrentToken();

        if (startTok.isStructStart()) {
            StringWriter wrt = new StringWriter(4096);

            JsonGenerator gen = factory.createGenerator(wrt);

            JsonToken tok = startTok, endTok = startTok == START_ARRAY ? END_ARRAY : END_OBJECT;

            int cnt = 1;

            while (cnt > 0) {
                writeToken(tok, p, gen);

                tok = p.nextToken();

                if (tok == startTok)
                    cnt++;
                else if (tok == endTok)
                    cnt--;
            }

            gen.close();

            return wrt.toString();
        }

        return p.getValueAsString();
    }
}