package org.odpi.egeria.apache.hive.metastore.connector.listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/*
The follow class is a prototype of a Metastore Listener that could
publish Metastore Table events to a kafka topic.

Some design discussion points
1. Stateless.  The connection to kafka is closed after each event is sent.
   This avoids checking constantly if the connection is valid and ensuring
   that the connection is closed in the case of an erroru
 */
public class EgeriaHiveListener extends MetaStoreEventListener  {

    /*
    tHere seems to be a cyclic reference in the events passed to the listener that
    prevents us converting the event bean to JSON with a StackOverflow Exception
    The below class is used as a wrapper to avoid the cyclic reference problem and
    allow us to process the event on the other end by keeping the event type
     */
    class Event
    {
        String type;
        Table table;

        public Event(String type, Table table)
        {
            this.type= type;
            this.table = table;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Table getTable() {
            return table;
        }

        public void setTable(Table table) {
            this.table = table;
        }


    }


    private static final Logger LOGGER = LoggerFactory.getLogger(EgeriaHiveListener.class);
    private static final  ObjectMapper objMapper = new ObjectMapper();

    String kafkaTopic;
    Properties kafkaProps = new Properties();

    public EgeriaHiveListener(Configuration config) {
        super(config);

        /*
        support minimum of connection properties
         */
        kafkaTopic = config.get("KafkaTopic", "METASTORE_EVENTS");
        kafkaProps.put("bootstrap.servers",config.get("KafkaBootStrap", "localhost:9092"));
        kafkaProps.put("acks", config.get("kafka_acks", "all"));
        kafkaProps.put("retries", config.get("kafka_retries", "0"));
        kafkaProps.put("key.serializer",  config.get("kafka_key.serializer","org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put("value.serializer",  config.get("kafka_value.serializer","org.apache.kafka.common.serialization.StringSerializer"));
        logWithHeader(" created ");
    }

    @Override
    public void onCreateTable(CreateTableEvent event) {
        Event egeriaEvent = new Event("CREATE_TABLE",event.getTable());
        String json = objToStr(egeriaEvent);
        logWithHeader(json);
        send(json);
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        Event egeriaEvent = new Event("ALTER_TABLE",event.getNewTable());
        String json = objToStr(egeriaEvent);
        logWithHeader(json);
        send(json);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        Event egeriaEvent = new Event("DROP_TABLE",tableEvent.getTable());
        String json = objToStr(egeriaEvent);
        logWithHeader(json);
        send(json);
    }

    private void logWithHeader(String obj){
        LOGGER.trace("[EgeriaListener][Thread: " + Thread.currentThread().getName()+"] | " + obj);
    }

    /*
    converts object to json
     */
    private String objToStr(Object obj){
        try {
            return objMapper.writeValueAsString(obj);
        } catch (IOException e) {
            LOGGER.error("Error on conversion", e);
        }
        return null;
    }

    /*
    What is the preferred approach to Exception handling for a metastore listener ?
     */
    private void send( String event) {
        /*
        we don't want any of these exceptions escaping
        just because a broker isn't available
         */
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps))
        {

            producer.send(new ProducerRecord<>(kafkaTopic, event));
            producer.flush();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
