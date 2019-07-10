package com.grpc.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grpc.server.config.Employees;
import com.grpc.server.config.JsonPOJODeserializer;
import com.grpc.server.config.JsonPOJOSerializer;
import com.grpc.server.domain.Employee;
import com.grpc.server.proto.EmployeeProducerServiceGrpc;
import com.grpc.server.proto.Messages;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;


public class EmployeeProducerService extends EmployeeProducerServiceGrpc.EmployeeProducerServiceImplBase {

    static final String NAME_TOPIC = "employee-topic-1";
    static final String SERVERS = "10.0.102.166:9092";
//     static final String SERVERS = System.getenv("HOST_IP");
//    static final String SERVERS="kafka:9092";
//    static final String SERVERS = "localhost:9092";

    static Properties properties;
    KafkaProducer<String,String> producer;
    private Queue<Employee> queue = new LinkedBlockingQueue<>();
    Random random = new Random();
    public EmployeeProducerService() {

        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS );
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("HOST_IP"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producer = new KafkaProducer<String,String>(properties);
//        produce(producer);
    }

    @Override
    public StreamObserver<Messages.EmployeeRequest> saveAll(StreamObserver<Messages.EmployeeSuccessResponse> responseObserver) {
        return new StreamObserver<Messages.EmployeeRequest>() {

            @Override
            public void onNext(Messages.EmployeeRequest v) {
                try{

                    ObjectMapper Obj = new ObjectMapper();
                    String jsonStr = Obj.writeValueAsString(v.getEmployee());
                    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(NAME_TOPIC,jsonStr );
                    producer.send(producerRecord);
                    System.out.println(producerRecord.toString());

                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                } finally {
                    producer.close();
                }
            }

            @Override
            public void onError(Throwable thrwbl) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void onCompleted() {
                System.out.println("Successfully persisted the kafka messsage");
                responseObserver.onNext(
                        Messages.EmployeeSuccessResponse.newBuilder()
                                .setIsOk(true)
                                .build());
                responseObserver.onCompleted();
            }
        };
    }

    public static void produce( KafkaProducer<String,String> producer) {
        try{

            ProducerRecord<String,String> producerRecord = null;
            Random random = new Random();
            for( int i = 0; i < 1 ;i++) {
                for (Employee e : Employees.getEmployees()) {
                    e.setId(random.nextInt(1000));
                    ObjectMapper Obj = new ObjectMapper();
                    String jsonStr = Obj.writeValueAsString(e);
                    producerRecord = new ProducerRecord<>(NAME_TOPIC,jsonStr );
                    producer.send(producerRecord);
                    System.out.println(producerRecord.toString());

//                System.out.println("Sending records -> " + i + " - " + producerRecord.key() + " - " + producerRecord.value() );
                }
//                Thread.sleep(10000);
            }


//                new ProducerRecord<>("topic",p)

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            producer.close();
        }
    }

}
