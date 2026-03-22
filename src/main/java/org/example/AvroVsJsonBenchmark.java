package org.example;

import org.apache.avro.io.*;
import org.apache.avro.reflect.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;

public class AvroVsJsonBenchmark {

    public static void main(String[] args) throws Exception {

        int N = 100_000;

        List<User> users = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            users.add(new User(
                    i,
                    "User_" + i,
                    "user" + i + "@example.com",
                    20 + (i % 50)
            ));
        }

        // ======================
        // AVRO SERIALIZATION (POJO)
        // ======================
        long avroStart = System.currentTimeMillis();

        ByteArrayOutputStream avroOut = new ByteArrayOutputStream();

        ReflectDatumWriter<User> avroWriter =
                new ReflectDatumWriter<>(User.class);

        BinaryEncoder encoder =
                EncoderFactory.get().binaryEncoder(avroOut, null);

        for (User u : users) {
            avroWriter.write(u, encoder);
        }
        encoder.flush();

        byte[] avroBytes = avroOut.toByteArray();

        long avroTime = System.currentTimeMillis() - avroStart;

        // ======================
        // JSON SERIALIZATION
        // ======================
        ObjectMapper mapper = new ObjectMapper();

        long jsonStart = System.currentTimeMillis();

        ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();

        for (User u : users) {
            jsonOut.write(mapper.writeValueAsBytes(u));
        }

        byte[] jsonBytes = jsonOut.toByteArray();

        long jsonTime = System.currentTimeMillis() - jsonStart;

        // ======================
        // RESULTS
        // ======================
        System.out.println("===== SIZE =====");
        System.out.println("Avro: " + avroBytes.length + " bytes");
        System.out.println("JSON: " + jsonBytes.length + " bytes");
        System.out.println("Ratio (JSON/Avro): " +
                (double) jsonBytes.length / avroBytes.length);

        System.out.println("\n===== TIME =====");
        System.out.println("Avro: " + avroTime + " ms");
        System.out.println("JSON: " + jsonTime + " ms");

        // ======================
        // FAIL-FAST DEMO
        // ======================
        System.out.println("\n===== FAIL-FAST TEST =====");

        try {
            User bad = new User();
            bad.id = 1;
            bad.name = "Test";
            bad.email = null; // <-- problematic for Avro if schema expects string
            bad.age = 25;

            avroWriter.write(bad, encoder); // often fails here
            encoder.flush();

        } catch (Exception e) {
            System.out.println("Avro failed fast:");
            System.out.println(e.getMessage());
        }

        try {
            User bad = new User(1, "Test", null, 25);

            String json = mapper.writeValueAsString(bad);

            System.out.println("\nJSON accepted:");
            System.out.println(json);

        } catch (Exception e) {
            System.out.println("JSON failed:");
            System.out.println(e.getMessage());
        }
    }
}