package com.dksou.essql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.util.Map;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchSchemaFactory  implements SchemaFactory {
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final Map map = (Map) operand;
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        try {
            final Map<String, Integer> esAddresses =
                    mapper.readValue((String) map.get("esAddresses"),
                            new TypeReference<Map<String, Integer>>() { });
            final Map<String, String> esClusterConfig =
                    mapper.readValue((String) map.get("esClusterConfig"),
                            new TypeReference<Map<String, String>>() { });
            final String esIndexName = (String) map.get("esIndexName");
            return new ElasticsearchSchema(esAddresses, esClusterConfig, esIndexName);
        } catch (IOException e) {
            throw new RuntimeException("Cannot parse values from json", e);
        }
    }
}
