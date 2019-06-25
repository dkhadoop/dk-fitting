package com.dksou.essql;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.dksou.essql.utils.ElasticsearchUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.Map;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchSchema extends AbstractSchema {
    private transient Client client;
    final String index;

    ElasticsearchSchema(Map<String, Integer> esAddresses,
                        Map<String, String> esConfig, String esIndexName) {
        super();
        client = new ElasticsearchUtil().getEsClient(esAddresses, esConfig);
        if (client != null) {
            final String[] indices = client.admin().indices()
                    .getIndex(new GetIndexRequest().indices(esIndexName))
                    .actionGet().getIndices();
            if (indices.length == 1) {
                index = indices[0];
            } else {
                index = null;
            }
        } else {
            index = null;
        }

    }



    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        try {
            GetMappingsResponse response = client.admin().indices().getMappings(
                    new GetMappingsRequest().indices(index)).get();
            ImmutableOpenMap<String, MappingMetaData> mapping = response.getMappings().get(index);
            for (ObjectObjectCursor<String, MappingMetaData> c: mapping) {
                builder.put(c.key, new ElasticsearchTable(client, index, c.key));
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return builder.build();
    }
}


