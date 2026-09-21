package com.pad.broker.net;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pad.broker.model.Message;

/** Centralizes Jackson usage so the rest of the codebase depends on this class, not on ObjectMapper directly. */
public class JsonCodec {

    private final ObjectMapper mapper = new ObjectMapper();

    public String encode(Message message) throws JsonProcessingException {
        return mapper.writeValueAsString(message);
    }

    public Message decode(String line) throws JsonProcessingException {
        return mapper.readValue(line, Message.class);
    }
}
