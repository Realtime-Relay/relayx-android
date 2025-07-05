package com.relay.realtime.models;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RequestBody {
    public String api_key;
    public Long l;
    public Boolean b;
    public List<String> strings;
    public Integer[] ints;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestBody requestBody = (RequestBody) o;
        return Objects.equals(api_key, requestBody.api_key) && Objects.equals(l, requestBody.l) && Objects.equals(b, requestBody.b) && Objects.equals(strings, requestBody.strings) && Arrays.equals(ints, requestBody.ints);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(api_key, l, b, strings);
        result = 31 * result + Arrays.hashCode(ints);
        return result;
    }
}