package com.grpc.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.grpc.server.domain.Employee;
import com.grpc.server.proto.EmployeeProducerServiceGrpc;
import com.grpc.server.proto.Messages;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class EmployeeProducerService extends EmployeeProducerServiceGrpc.EmployeeProducerServiceImplBase {

    static final String NAME_TOPIC = "employee-topic-1";
//    static final String SERVERS = "10.0.102.166:9092";
     static final String SERVERS = System.getenv("HOST_IP");
//    static final String SERVERS="kafka:9092";
//    static final String SERVERS = "localhost:9092";

    static Properties properties;
    public EmployeeProducerService() {

        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS );
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("HOST_IP"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

    }


    @Override
    public void save(Messages.EmployeeRequest request, StreamObserver<Messages.EmployeeSuccessResponse> responseObserver) {

        try(KafkaProducer<String,String> producer = new KafkaProducer<>(properties)){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(NAME_TOPIC,json(request) );
            producer.send(producerRecord);
            System.out.println("Messge Persisted => " + producerRecord.toString());
            Messages.EmployeeSuccessResponse response =
                    Messages.EmployeeSuccessResponse.newBuilder()
                            .setIsOk(true)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        responseObserver.onError(new Exception(" Cannot persist the data " +
                request.getEmployee()));
    }

    private String json(Messages.EmployeeRequest request) {
        String jsonStr = "";
        try {
        ObjectMapper Obj = new ObjectMapper();
        Employee employee = new Employee();
        employee.setId(request.getEmployee().getId());
        employee.setBadgeNumber(request.getEmployee().getBadgeNumber());
        employee.setFirstName(request.getEmployee().getFirstName());
        employee.setLastName(request.getEmployee().getLastName());
        jsonStr = Obj.writeValueAsString(employee);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonStr;
    }

}
