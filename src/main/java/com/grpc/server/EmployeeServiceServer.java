package com.grpc.server;

import com.grpc.server.service.EmployeeConsumerService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class EmployeeServiceServer {
    public static void main(String[] args) {
        try {
            EmployeeServiceServer employeeServiceServer = new EmployeeServiceServer();
            employeeServiceServer.start();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    private Server server;
    
    private void start() throws Exception {
        final int port = 9000;
        EmployeeConsumerService service = new EmployeeConsumerService();

        server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();

        System.out.println("Listening on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down server");
                EmployeeServiceServer.this.stop();
            }
        });
        
        server.awaitTermination();
        
    }
    
    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
