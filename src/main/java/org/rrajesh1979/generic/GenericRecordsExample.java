package org.rrajesh1979.generic;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

@Slf4j
public class GenericRecordsExample {
    public static void main(String[] args) {
        // Step 0: Define Schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("""
                {
                     "type": "record",
                     "namespace": "com.example",
                     "name": "Customer",
                     "doc": "Avro Schema for our Customer",
                     "fields": [
                       { "name": "first_name", "type": "string", "doc": "First Name of Customer" },
                       { "name": "last_name", "type": "string", "doc": "Last Name of Customer" },
                       { "name": "age", "type": "int", "doc": "Age at the time of registration" },
                       { "name": "height", "type": "float", "doc": "Height at the time of registration in cm" },
                       { "name": "weight", "type": "float", "doc": "Weight at the time of registration in kg" },
                       { "name": "automated_email", "type": "boolean", "default": true, "doc": "Field indicating if the user is enrolled in marketing emails" }
                     ]
                }
                """);

        // Step 1: Create a GenericRecord
        // we build our first customer
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 26);
        customerBuilder.set("height", 175f);
        customerBuilder.set("weight", 70.5f);
        customerBuilder.set("automated_email", false);
        GenericRecord myCustomer = customerBuilder.build();
        log.info("myCustomer: {}", myCustomer);

        // we build our second customer which has defaults
        GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
        customerBuilderWithDefault.set("first_name", "John");
        customerBuilderWithDefault.set("last_name", "Doe");
        customerBuilderWithDefault.set("age", 26);
        customerBuilderWithDefault.set("height", 175f);
        customerBuilderWithDefault.set("weight", 70.5f);
        GenericRecord customerWithDefault = customerBuilderWithDefault.build();
        log.info("customerWithDefault: {}", customerWithDefault);

        // we start triggering errors
        GenericRecordBuilder customerWrong = new GenericRecordBuilder(schema);
        // non existent field
//        customerWrong.set("new_field", "foobar");
        // missing required field
        customerWrong.set("height", "blahblah");
//        customerWrong.set("first_name", "John");
        customerWrong.set("last_name", "Doe");
        customerWrong.set("age", 26);
        customerWrong.set("weight", 26f);
        customerWrong.set("automated_email", 70);
        try {
            GenericRecord wrong = customerWrong.build();
            log.info("wrong schema customer: {}", wrong);
        } catch (AvroRuntimeException e){
            log.error("Error: {}", e.getMessage());
            e.printStackTrace();
        }

        // Step 2: Write GenericRecord to a file
        // writing to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(myCustomer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(myCustomer);
            log.info("Wrote customer-generic.avro");
        } catch (IOException e) {
            log.error("Error writing avro file: {}", e.getMessage());
            e.printStackTrace();
        }

        // Step 3: Read GenericRecord from a file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            customerRead = dataFileReader.next();
            log.info("Read customer: {}", customerRead);

            // get the data from the generic record
            log.info("First name: {}", customerRead.get("first_name"));

            // read a non-existent field
            log.info("Non existent field: {}", customerRead.get("new_field"));
        }
        catch(IOException e) {
            log.error("Error reading avro file: {}", e.getMessage());
        }


        // Step 4: Interpret GenericRecord
    }
}
