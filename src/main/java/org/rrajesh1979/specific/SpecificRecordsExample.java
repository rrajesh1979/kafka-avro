package org.rrajesh1979.specific;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.rrajesh1979.Customer;

import java.io.File;
import java.io.IOException;

@Slf4j
public class SpecificRecordsExample {
    public static void main(String[] args) {
        // Step 0: Define Schema

        // Step 1: Create a SpecificRecord
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        customerBuilder.setAge(30);
        customerBuilder.setHeight(1.75f);
        customerBuilder.setWeight(80);
        customerBuilder.setAutomatedEmail(false);
        Customer customer = customerBuilder.build();
        log.info("Customer: {}", customer);

        // Step 2: Write SpecificRecord to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            log.info("successfully wrote customer-specific.avro");
        } catch (IOException e){
            log.error("Error writing customer-specific.avro, {}", e.getMessage());
        }

        // Step 3: Read SpecificRecord from a file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            log.info("Reading our specific record");
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                log.info("Read customer: {}", readCustomer);
                log.info("First name: {}", readCustomer.getFirstName());
            }
        } catch (IOException e) {
            log.error("Error reading customer-specific.avro, {}", e.getMessage());
        }

        // Step 4: Interpret SpecificRecord
    }
}
