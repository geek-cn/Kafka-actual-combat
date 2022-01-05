package com.jx.kafak.producer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @Author jx
 * @Date 2021/12/24
 * 自定义的序列化器
 */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Company company) {
        if (company == null) {
            return null;
        }

        byte[] name,address;
        if (company.getName() != null) {
           name = company.getName().getBytes(StandardCharsets.UTF_8);
        } else {
           name =  new byte[0];
        }
        if (company.getAddress() != null) {
           address = company.getAddress().getBytes(StandardCharsets.UTF_8);
        } else {
            address = new byte[0];
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
        byteBuffer.putInt(name.length);
        byteBuffer.put(name);
        byteBuffer.putInt(address.length);
        byteBuffer.put(address);
        return byteBuffer.array();
    }

    @Override
    public void close() {

    }
}
