package com.jx.kafak.protostuff;

import com.jx.kafak.producer.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/28
 */
public class ProtostuffDeserialzer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            System.out.println("=======null======");
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data,company,schema);

        return company;
    }


    @Override
    public void close() {

    }
}
