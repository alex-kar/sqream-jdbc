package com.sqream.jdbc.connector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.sqream.jdbc.utils.Utils.decode;

class SQSocketConnector extends SQSocket {

    private static final byte PROTOCOL_VERSION = 7;
    private static final int HEADER_SIZE = 10;
    private static final List<Byte> SUPPORTED_PROTOCOLS = new ArrayList<>(Arrays.asList((byte)6, (byte)7));

    private ByteBuffer responseMessage = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);;
    private ByteBuffer header = ByteBuffer.allocateDirect(10).order(ByteOrder.LITTLE_ENDIAN);

    SQSocketConnector(String ip, int port) throws IOException, NoSuchAlgorithmException {
        super(ip, port);
    }

    // (2)  /* Return ByteBuffer with appropriate header for message */
    ByteBuffer generateHeaderedBuffer(long data_length, boolean is_text_msg) {

        return ByteBuffer.allocate(10 + (int) data_length).order(ByteOrder.LITTLE_ENDIAN).put(PROTOCOL_VERSION).put(is_text_msg ? (byte)1:(byte)2).putLong(data_length);
    }

    // (3)  /* Used by _send_data()  (merge if only one )  */
    int getParseHeader() throws IOException, ConnectorImpl.ConnException {

        header.clear();
        readData(header, HEADER_SIZE);

        //print ("header: " + header);
        if (!SUPPORTED_PROTOCOLS.contains(header.get()))
            throw new ConnectorImpl.ConnException("bad protocol version returned - " + PROTOCOL_VERSION + " perhaps an older version of SQream or reading out of oreder");

        byte is_text = header.get();  // Catching the 2nd byte of a response
        long response_length = header.getLong();

        return (int)response_length;
    }

    // (4) /* Manage actual sending and receiving of ByteBuffers over exising socket  */
    String sendData(ByteBuffer data, boolean get_response) throws IOException, ConnectorImpl.ConnException {
        /* Used by _send_message(), _flush()   */

        if (data != null ) {
            data.flip();
            int written;
            while(data.hasRemaining()) {
                super.write(data);
            }
        }


        // Sending null for data will get us here directly, allowing to only get socket response if needed
        if(get_response) {
            int msg_len = getParseHeader();
            if (msg_len > 64000) // If our 64K response_message buffer doesn't do
                responseMessage = ByteBuffer.allocate(msg_len);
            responseMessage.clear();
            responseMessage.limit(msg_len);
            readData(responseMessage, msg_len);
        }

        return (get_response) ? decode(responseMessage) : "" ;
    }

    // (5)   /* Send a JSON string to SQream over socket  */

    String sendMessage(String message, boolean get_response) throws IOException, ConnectorImpl.ConnException {

        byte[] message_bytes = message.getBytes();
        ByteBuffer message_buffer = generateHeaderedBuffer((long)message_bytes.length, true);
        message_buffer.put(message_bytes);

        return sendData(message_buffer, get_response);
    }

    int readData(ByteBuffer response, int msg_len) throws IOException, ConnectorImpl.ConnException {
        /* Read either a specific amount of data, or until socket is empty if msg_len is 0.
         * response ByteBuffer of a fitting size should be supplied.
         */
        if (msg_len > response.capacity())
            throw new ConnectorImpl.ConnException("Attempting to read more data than supplied bytebuffer allows");

        int total_bytes_read = 0;

        while (total_bytes_read < msg_len || msg_len == 0) {
            int bytes_read = super.read(response);
            if (bytes_read == -1)
                throw new IOException("Socket closed. Last buffer written: " + response);
            total_bytes_read += bytes_read;

            if (msg_len == 0 && bytes_read == 0)
                break;  // Drain mode, read all that was available
        }

        response.flip();  // reset position to allow reading from buffer


        return total_bytes_read;
    }
}
