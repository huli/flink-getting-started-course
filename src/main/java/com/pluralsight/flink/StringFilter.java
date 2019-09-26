package com.pluralsight.flink;

public class StringFilter implements org.apache.flink.api.common.functions.FilterFunction<String> {
    @Override
    public boolean filter(String value) throws Exception {

        try{
            Double.parseDouble(value.trim());
            return true;
        }catch(Exception ex){
            return false;
        }

    }
}
