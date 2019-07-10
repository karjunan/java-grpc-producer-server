package com.grpc.server.service;

import com.grpc.server.config.JsonPOJODeserializer;
import com.grpc.server.config.JsonPOJOSerializer;
import com.grpc.server.domain.Employee;
import com.grpc.server.proto.EmployeeConsumerServiceGrpc;
import com.grpc.server.proto.Messages;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


public class EmployeeConsumerService extends EmployeeConsumerServiceGrpc.EmployeeConsumerServiceImplBase {

    static final String NAME_TOPIC = "employee-topic-1";
//    static final String SERVERS = "10.0.102.166:9092";
//     static final String SERVERS = System.getenv("HOST_IP");
//    static final String SERVERS="kafka:9092";
//    static final String SERVERS = "localhost:9092";

    static Properties properties;

    private Queue<Employee> queue = new LinkedBlockingQueue<>();

    public EmployeeConsumerService() {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "name-read-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  System.getenv("HOST_IP"));
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Employee> employeeSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Employee.class);
        employeeSerializer.configure(serdeProps, false);

        final Deserializer<Employee> employeeDeSerializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Employee.class);
        employeeDeSerializer.configure(serdeProps, false);

        final Serde<Employee> employeeSerde = Serdes.serdeFrom(employeeSerializer, employeeDeSerializer);
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,new JsonDeserializer());
        StreamsBuilder nameAgeBuilder = new StreamsBuilder();
        System.out.println("Current properties => " + properties);

        KStream<String, Employee> name = nameAgeBuilder.stream(NAME_TOPIC, Consumed.with(Serdes.String(),employeeSerde ));
        name.foreach((k,v) -> {
//            System.out.println(v);
            queue.add(v);
        });

        KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(), properties);
        kafkaStreams.start();
    }


    @Override
    public void getAll(Messages.GetAllRequest request,
                       StreamObserver<Messages.EmployeeResponse> responseObserver) {

        System.out.println("Received a message to start stream processing");
//        System.out.println(queue);
        while(true) {
            queue.forEach(v-> {
                System.out.println(v);
                Messages.Employee emp = Messages.Employee.newBuilder()
                        .setId(v.getId())
                        .setFirstName(v.getFirstName())
                        .setLastName(v.getLastName())
                        .setBadgeNumber(v.getBadgeNumber())
                        .build();

               Messages.EmployeeResponse response = Messages.EmployeeResponse.newBuilder()
                        .setEmployee(emp)
                        .build();

                responseObserver.onNext(response);
                queue.poll();
            });

        }
//        responseObserver.onCompleted();
    }


}
