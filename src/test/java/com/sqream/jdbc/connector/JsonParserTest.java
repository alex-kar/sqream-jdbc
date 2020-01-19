package com.sqream.jdbc.connector;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.sqream.jdbc.connector.JsonParser.TEXT_ITEM_SIZE;
import static org.junit.Assert.*;

public class JsonParserTest {

    private JsonParser parser = new JsonParser();

    @Test
    public void toConnectionStateTest() {
        int CONNECTION_ID = 123;
        String ENCODING = "cp874";
        String JSON = String.format(
                "{\"connectionId\":%s,\"databaseConnected\":\"databaseConnected\",\"varcharEncoding\":\"%s\"}",
                CONNECTION_ID, ENCODING);
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON);

        assertNotNull(result);
        assertEquals(expected.getConnectionId(), result.getConnectionId());
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void checkDefaultVarcharEncoding() {
        int CONNECTION_ID = 123;
        String DEFAULT_ENCODING = "ascii";
        String JSON_WITHOUT_ENCODING = String.format(
                "{\"connectionId\":%s,\"databaseConnected\":\"databaseConnected\"}", CONNECTION_ID);
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, DEFAULT_ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON_WITHOUT_ENCODING);

        assertNotNull(result);
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void checkCP874VarcharEncoding() {
        int CONNECTION_ID = 123;
        String ENCODING_CONTAINS_874 = "someEncodingContains874-*&%#$@";
        String EXPECTED_ENCODING = "cp874";
        String JSON = String.format(
                "{\"connectionId\":%s,\"databaseConnected\":\"databaseConnected\",\"varcharEncoding\":\"%s\"}",
                CONNECTION_ID, ENCODING_CONTAINS_874);
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, EXPECTED_ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON);

        assertNotNull(result);
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void whenQueryTypeInIsEmptyArrayTest() {
        String json = "{\"queryType\":[]}";

        List<ColumnMetadataDto> resultList = parser.toQueryTypeInput(json);

        assertNotNull(resultList);
        assertTrue(resultList.isEmpty());
    }

    @Test
    public void toQueryTypeInputTest() {
        String JSON = "{" +
                "  \"queryType\": [" +
                "    {" +
                "      \"name\": \"someName\"," +
                "      \"isTrueVarChar\": false," +
                "      \"nullable\": true," +
                "      \"type\": [" +
                "        \"ftBool\"," +
                "        1," +
                "        0" +
                "      ]" +
                "    }," +
                "    {" +
                "      \"isTrueVarChar\": true," +
                "      \"nullable\": false," +
                "      \"type\": [" +
                "        \"ftBlob\"," +
                "        0," +
                "        0" +
                "      ]" +
                "    }" +
                "  ]" +
                "}";
        List<ColumnMetadataDto> expectedList = new LinkedList<>();
        expectedList.add(new ColumnMetadataDto(false, "someName", true, "ftBool", 1));
        // name = "" by default
        // if itemSize == 0 then set as TEXT_ITEM_SIZE
        expectedList.add(new ColumnMetadataDto(true, "", false, "ftBlob", TEXT_ITEM_SIZE));

        List<ColumnMetadataDto> resultList = parser.toQueryTypeInput(JSON);

        assertEquals(expectedList.size(), resultList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertEquals(expectedList.get(i).getName(), resultList.get(i).getName());
            assertEquals(expectedList.get(i).getValueType(), resultList.get(i).getValueType());
            assertEquals(expectedList.get(i).getValueSize(), resultList.get(i).getValueSize());
            assertEquals(expectedList.get(i).isNullable(), resultList.get(i).isNullable());
            assertEquals(expectedList.get(i).isTruVarchar(), resultList.get(i).isTruVarchar());
        }
    }

    @Test
    public void whenQueryTypeOutIsEmptyArrayTest() {
        String json = "{\"queryTypeNamed\":[]}";

        List<ColumnMetadataDto> resultList = parser.toQueryTypeOut(json);

        assertNotNull(resultList);
        assertTrue(resultList.isEmpty());
    }
}