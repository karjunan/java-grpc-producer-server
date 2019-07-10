package com.grpc.server.config;

import com.grpc.server.domain.Employee;

import java.util.ArrayList;
import java.util.List;

public class Employees {


    public static List<Employee> getEmployees() {
        List<Employee> list = new ArrayList<>();

        Employee employee = new Employee();
        employee.setId(1);
        employee.setBadgeNumber(2080);
        employee.setFirstName("Grace");
        employee.setLastName("Decker");
        list.add(employee);

        employee = new Employee();
        employee.setId(2);
        employee.setBadgeNumber(2081);
        employee.setFirstName("Krishnakumar");
        employee.setLastName("Arjunan");
        list.add(employee);

        employee = new Employee();
        employee.setId(3);
        employee.setBadgeNumber(2082);
        employee.setFirstName("Sachin");
        employee.setLastName("Tendulkar");
        list.add(employee);

        employee = new Employee();
        employee.setId(4);
        employee.setBadgeNumber(2083);
        employee.setFirstName("Mark");
        employee.setLastName("Waugh");
        list.add(employee);

        employee = new Employee();
        employee.setId(5);
        employee.setBadgeNumber(2084);
        employee.setFirstName("Brain");
        employee.setLastName("Lara");
        list.add(employee);

        return list;
    }
    
}
