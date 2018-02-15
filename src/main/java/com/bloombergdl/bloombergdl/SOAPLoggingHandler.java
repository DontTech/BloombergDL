package com.bloombergdl.bloombergdl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.handler.soap.SOAPHandler;
import javax.xml.ws.handler.soap.SOAPMessageContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * This simple SOAPHandler will output the contents of incoming
 * and outgoing messages.
 */
public class SOAPLoggingHandler implements SOAPHandler<SOAPMessageContext> {

    private final static Log LOGGER = LogFactory.getLog(BloombergDL.class);

    // change this to redirect output if desired
    // private static final PrintStream out = System.out;
    private static final ByteArrayOutputStream OUT = new ByteArrayOutputStream();

    @Override
    public Set<QName> getHeaders() {
        return null;
    }

    @Override
    public boolean handleMessage(SOAPMessageContext smc) {
        logToStream(smc);
        return true;
    }

    @Override
    public boolean handleFault(SOAPMessageContext smc) {
        logToStream(smc);
        return true;
    }

    // nothing to clean up
    @Override
    public void close(MessageContext messageContext) {
    }

    /*
     * Check the MESSAGE_OUTBOUND_PROPERTY in the context
     * to see if this is an outgoing or incoming message.
     * Write a brief message to the print stream and
     * output the message. The writeTo() method can throw
     * SOAPException or IOException
     */
    private void logToStream(SOAPMessageContext smc) {
        if ((boolean) smc.get(MessageContext.MESSAGE_OUTBOUND_PROPERTY)) {
            LOGGER.info("Outbound message:");
        } else {
            LOGGER.info("Inbound message:");
        }

        SOAPMessage message = smc.getMessage();
        try {
            message.writeTo(OUT);
            LOGGER.info(OUT);
        }
        catch (SOAPException | IOException ex) {
            LOGGER.error("Exception in handler: " + ex);
        }
    }
}
