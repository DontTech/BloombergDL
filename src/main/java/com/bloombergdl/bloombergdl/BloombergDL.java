package com.bloombergdl.bloombergdl;

import com.bloomberg.datalic.dlws.stubs.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Scanner;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.Binding;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Holder;
import javax.xml.ws.handler.Handler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Bloomberg Data License WSDL Client
 */
public class BloombergDL {

    private final static String HISTRESPID = "historyresponseid";
    private final static String DATARESPID = "dataresponseid";
    private final static String INPUTFILE = "inputfile";
    private final static String KEYSTORE = "keystore";
    private final static String PASSWORD = "keystorepass";
    private final static String TYPE = "keystoretype";
    private final static String RETRYWAIT = "retrywait";
    private final static Log LOGGER = LogFactory.getLog(BloombergDL.class);

    /**
     * Main program.
     *
     * Queries web services provided for Bloomberg Data License holder for requested data.
     *
     * @param args arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        LOGGER.debug("Started execution with args = " + Arrays.toString(args));
        Option hist_resp_opt = Option.builder("hr").longOpt(HISTRESPID).hasArg()
                .desc("Response Id for a of a previous run of GetHistory")
                .argName("ID").required(false).build();
        Option data_resp_opt = Option.builder("dr").longOpt(DATARESPID).hasArg()
                .desc("Response Id for a of a previous run of GetHistory")
                .argName("ID").required(false).build();
        Option input_file_opt = Option.builder("i").longOpt(INPUTFILE).hasArg()
                .desc("Input file containing list of fields.")
                .argName("FILE").required(false).build();
        Option keystore_opt = Option.builder("k").longOpt(KEYSTORE).hasArg()
                .desc("keystore containing DLWS certificate. Default: " + PerSecurity.KYST)
                .argName("FILE").required(false).build();
        Option password_opt = Option.builder("p").longOpt(PASSWORD).hasArg()
                .desc("Password of the keystore. Default: " + PerSecurity.PASS)
                .argName("PASSWORD").required(false).build();
        Option type_opt = Option.builder("t").longOpt(TYPE).hasArg()
                .desc("Type of the keystore (e.g. PKCS12, JKS). Default: " + PerSecurity.TYPE)
                .argName("TYPE").required(false).build();
        Option retry_wait_opt = Option.builder("r").longOpt(RETRYWAIT).hasArg()
                .desc("Seconds to wait before retrying. Cannot be less that 5. Default: " + PerSecurity.POLL_FREQUENCY)
                .argName("SECONDS").required(false).build();

        Options options = new Options();
        options.addOption(hist_resp_opt);
        options.addOption(data_resp_opt);
        options.addOption(input_file_opt);
        options.addOption(keystore_opt);
        options.addOption(password_opt);
        options.addOption(type_opt);
        options.addOption(retry_wait_opt);

        String hist_response_id = "";
        String data_response_id = "";
        String keystore = PerSecurity.KYST;
        String password = PerSecurity.PASS;
        String type = PerSecurity.TYPE;
        String input_file = "";
        int retry_wait = PerSecurity.POLL_FREQUENCY;

        boolean resume_hist = false;
        boolean resume_data = false;
        boolean get_hist = true;
        boolean get_data = true;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine command_line = parser.parse(options, args);

            if (command_line.hasOption(HISTRESPID)) {
                hist_response_id = command_line.getOptionValue(HISTRESPID);
                LOGGER.info("Resuming retrieving history with response id = " + hist_response_id);
                if ("0".equals(hist_response_id)) {
                    get_hist = false;
                } else {
                    resume_hist = true;
                }
            }

            if (command_line.hasOption(DATARESPID)) {
                data_response_id = command_line.getOptionValue(DATARESPID);
                LOGGER.info("Resuming retrieving data with response id = " + data_response_id);
                if ("0".equals(data_response_id)) {
                    get_data = false;
                } else {
                    resume_data = true;
                }
            }

            if (!get_data && !get_hist) {
                LOGGER.warn("Both " + HISTRESPID + " and " + DATARESPID + " are to be skipped? Quitting instead ...");
                System.exit(0);
            }

            if (command_line.hasOption(INPUTFILE)) {
                input_file = command_line.getOptionValue(INPUTFILE);
                LOGGER.debug("Using " + INPUTFILE + " : " + input_file);
                if (resume_data || resume_hist) {
                    System.err.println("Input File not Required");
                    System.exit(0);
                }
            } else {
                System.out.println("No " + INPUTFILE + " specified.");
            }

            if (command_line.hasOption(KEYSTORE)) {
                keystore = command_line.getOptionValue(KEYSTORE);
                LOGGER.debug("Using " + KEYSTORE + " : " + keystore);
            }

            if (command_line.hasOption(PASSWORD)) {
                password = command_line.getOptionValue(PASSWORD);
                LOGGER.debug("Using " + PASSWORD + " : " + password);
            }

            if (command_line.hasOption(TYPE)) {
                type = command_line.getOptionValue(TYPE);
                LOGGER.debug("Using " + TYPE + " : " + type);
            }

            if (command_line.hasOption(RETRYWAIT)) {
                LOGGER.debug("Using " + RETRYWAIT + " from command line.");
                retry_wait = Integer.valueOf(command_line.getOptionValue(RETRYWAIT));
                if (retry_wait < 5) {
                    LOGGER.info(RETRYWAIT + " was " + retry_wait + " which is less than 5. Resetting to 5.");
                    retry_wait = 5;
                }
            }
        } catch (ParseException ex) {
            System.err.println(ex.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(BloombergDL.class.getSimpleName(), options);
            LOGGER.fatal(ex);
            System.exit(1);
        }

        System.setProperty("javax.net.ssl.keyStore", keystore);
        System.setProperty("javax.net.ssl.keyStorePassword", password);
        System.setProperty("javax.net.ssl.keyStoreType", type);

        LOGGER.info("Starting ...");
        PerSecurityWS_Service service = new PerSecurityWS_Service();
        PerSecurityWS port = service.getPerSecurityWSPort();
        LOGGER.info("Connection established");

        RetrieveGetHistoryRequest hist_request = new RetrieveGetHistoryRequest();
        RetrieveGetDataRequest data_request = new RetrieveGetDataRequest();
        try {
            if (get_hist && !resume_hist) {
                LOGGER.info("Submitting history request ...");
                hist_response_id = submitGetHistoryRequest(port).value;
                System.out.println("-" + HISTRESPID + " " + hist_response_id);
            }
            if (get_data && !resume_data) {
                LOGGER.info("Submitting data request ...");
                data_response_id = submitGetDataRequest(port, input_file).value;
                System.out.println("-" + DATARESPID + " " + data_response_id);
            }

            hist_request.setResponseId(hist_response_id);
            data_request.setResponseId(data_response_id);
        } catch (DatatypeConfigurationException ex) {
            LOGGER.fatal(ex);
            System.exit(1);
        }
        LOGGER.debug("Enabling soap logging ...");
        Binding binding = ((BindingProvider) port).getBinding();
        List<Handler> handler_chain = binding.getHandlerChain();
        handler_chain.add(new SOAPLoggingHandler());
        binding.setHandlerChain(handler_chain);

        RetrieveGetHistoryResponse hist_response = new RetrieveGetHistoryResponse();
        RetrieveGetDataResponse data_response = new RetrieveGetDataResponse();
        boolean hist_retrieved = false;
        boolean data_retrieved = false;
        if (get_hist) {
            hist_response = port.retrieveGetHistoryResponse(hist_request);
            LOGGER.info(hist_response.getStatusCode().getCode() + " " + hist_response.getStatusCode().getDescription());
            hist_retrieved = (hist_response.getStatusCode().getCode() != PerSecurity.DATA_NOT_AVAILABLE);
        } else {
            LOGGER.debug("Disabling history retrieval");
        }
        if (get_data) {
            data_response = port.retrieveGetDataResponse(data_request);
            LOGGER.info(data_response.getStatusCode().getCode() + " " + data_response.getStatusCode().getDescription());
            data_retrieved = (data_response.getStatusCode().getCode() != PerSecurity.DATA_NOT_AVAILABLE);
        } else {
            LOGGER.debug("Disabling data retrieval");
        }

        int retry_attempt = 0;
        while ((get_hist && !hist_retrieved) || (get_data && !data_retrieved)) {
            ++retry_attempt;
            System.out.println("Retrying ... attempt = " + retry_attempt);
            LOGGER.info("Sleeping for " + retry_wait + " seconds.");
            try {
                Thread.sleep(retry_wait * 1000);
            } catch (InterruptedException ex) {
                LOGGER.fatal(ex);
                System.exit(1);
            }
            if (get_hist && !hist_retrieved) {
                hist_response = port.retrieveGetHistoryResponse(hist_request);
                LOGGER.info(hist_response.getStatusCode().getCode() + " " + hist_response.getStatusCode().getDescription());
                hist_retrieved = (hist_response.getStatusCode().getCode() != PerSecurity.DATA_NOT_AVAILABLE);
            }
            if (get_data && !data_retrieved) {
                data_response = port.retrieveGetDataResponse(data_request);
                LOGGER.info(data_response.getStatusCode().getCode() + " " + data_response.getStatusCode().getDescription());
                data_retrieved = (data_response.getStatusCode().getCode() != PerSecurity.DATA_NOT_AVAILABLE);
            }
        }

        if (hist_response.getStatusCode().getCode() == PerSecurity.SUCCESS) {
            LOGGER.info("Retrieve get history request successful.");
            LOGGER.debug("History response ID: " + hist_response.getResponseId());
        } else {
            System.err.println("History response wasn't SUCCESS");
        }

        if (data_response.getStatusCode().getCode() == PerSecurity.SUCCESS) {
            LOGGER.info("Retrieve get data request successful.");
            LOGGER.debug("Data response ID: " + data_response.getResponseId());
        } else {
            System.err.println("Data response wasn't SUCCESS");
        }
        System.out.println("Finished.");
    }

    private static Instruments GetInstrumentList() throws IOException {
        LOGGER.info("Loading ticker list.");
        Instruments instruments = new Instruments();
        InputStream is;
        is = BloombergDL.class.getResourceAsStream("/tickerlist.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            Instrument instrument = new Instrument();
            instrument.setId(line);
            instrument.setYellowkey(MarketSector.EQUITY);
            instruments.getInstrument().add(instrument);
        }
        return instruments;
    }

    private static Fields GetFieldList(String fileName) throws IOException {
        Fields fields = new Fields();
        InputStream is;
        if (fileName.isEmpty())
        {
            LOGGER.info("No field file specified (" + fileName + "), loading default field list.");
            is = BloombergDL.class.getResourceAsStream("/fields.txt");
        }
        else
        {
            LOGGER.info("Reading fields from " + fileName);
            is = new FileInputStream(fileName);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        List<String> fieldslist = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            fieldslist.add(line);
        }
        fields.getField().addAll(fieldslist);
        return fields;
    }

    private static Holder<String> submitGetHistoryRequest(PerSecurityWS port) throws DatatypeConfigurationException, IOException {
        Calendar start_cal = Calendar.getInstance();
        start_cal.add(Calendar.MONTH, -1);

        GregorianCalendar start_gc = new GregorianCalendar();
        start_gc.setTime(start_cal.getTime());
        XMLGregorianCalendar start = DatatypeFactory.newInstance().newXMLGregorianCalendar(start_gc);

        GregorianCalendar end_gc = new GregorianCalendar();
        Calendar end_cal = Calendar.getInstance();
        end_gc.setTime(end_cal.getTime());
        XMLGregorianCalendar end = DatatypeFactory.newInstance().newXMLGregorianCalendar(end_gc);

        Period period = new Period();
        period.setStart(start);
        period.setEnd(end);

        DateRange date_range = new DateRange();
        date_range.setPeriod(period);

        GetHistoryHeaders headers = new GetHistoryHeaders();
        headers.setDaterange(date_range);
        //headers.setUsernumber(USERNUMBER);
        //headers.setSn(SN);
        //headers.setWs(WS);
        headers.setHistCrncy("USD");

        Fields fields = new Fields();
        fields.getField().add("PX_LAST");

        Instruments instruments = GetInstrumentList();

        Holder<ResponseStatus> status_code = new javax.xml.ws.Holder<>();
        Holder<String> request_id = new javax.xml.ws.Holder<>();
        Holder<String> response_id = new javax.xml.ws.Holder<>();
        port.submitGetHistoryRequest(headers, fields, instruments, status_code, request_id, response_id);

        LOGGER.info(status_code.value.getCode() + " " + status_code.value.getDescription());
        LOGGER.info("Request Id: " + request_id.value);
        LOGGER.info("Response Id: " + response_id.value);
        return response_id;
    }

    private static Holder<String> submitGetDataRequest(PerSecurityWS port, String fileName) throws DatatypeConfigurationException, IOException {
        GetDataHeaders headers = new GetDataHeaders();
        headers.setSecmaster(true);
        headers.setClosingvalues(true);
        headers.setDerived(true);

        Fields fields = GetFieldList(fileName);

        Instruments instruments = GetInstrumentList();

        GregorianCalendar today_gc = new GregorianCalendar();
        Calendar today_cal = Calendar.getInstance();
        today_gc.setTime(today_cal.getTime());
        XMLGregorianCalendar today = DatatypeFactory.newInstance().newXMLGregorianCalendar(today_gc);

        BvalFieldSet fieldset = new BvalFieldSet();
        fieldset.setDate(today);
        fieldset.setFieldmacro(BvalFieldMacro.BVAL_ALL);
        BvalFieldSets fieldsets = new BvalFieldSets();
        fieldsets.getFieldset().add(fieldset);

        Holder<ResponseStatus> status_code = new javax.xml.ws.Holder<>();
        Holder<String> request_id = new javax.xml.ws.Holder<>();
        Holder<String> response_id = new javax.xml.ws.Holder<>();
        port.submitGetDataRequest(headers, fieldsets, fields, instruments, status_code, request_id, response_id);

        LOGGER.info(status_code.value.getCode() + " " + status_code.value.getDescription());
        LOGGER.info("Request Id: " + request_id.value);
        LOGGER.info("Response Id: " + response_id.value);
        return response_id;
    }
}
