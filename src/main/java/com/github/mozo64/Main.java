package com.github.mozo64;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Semaphore;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main {

    private static final String BUCKET_NAME = "apps-mozo";
    private static final String FILE = "test_file_3";
    private final static String EXTENSION_GZ = ".gz";
    private static final int GUID_PER_THREAD = 50_000;
    private final static Character LINE_SEPARATOR = '\n';
    private static final int WRITE_THREADS = 3;

    public static void main(String[] args) throws IOException, InterruptedException {

        AmazonS3Client s3client = getS3Client();
//        uploadTestFile(BUCKET_NAME, FILE, s3client, GUID_PER_THREAD);
//        uploadTestFileEasyVersion(BUCKET_NAME, FILE, s3client, GUID_PER_THREAD);
        uploadTestFileReallyEasyVersion(BUCKET_NAME, FILE, s3client, GUID_PER_THREAD);
        HashMap<Integer, HashMap<Long, BigDecimal>> fromS3 = getFromS3(BUCKET_NAME, FILE, s3client);

        System.out.print("This is the end");
    }

    private static AmazonS3Client getS3Client() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(Credentials.access_key_id, Credentials.secret_access_key);
        AmazonS3Client s3client = new AmazonS3Client(awsCreds);
        return s3client;
    }

    private static void uploadTestFile(String bucketName, String file, AmazonS3Client s3client, int guidPerThread) throws IOException, InterruptedException {
        int genThreads = 10;

        try (S3UploadStreamer uploadStreamer = new S3UploadStreamer(s3client, bucketName, file + EXTENSION_GZ, WRITE_THREADS)) {
            try (GZIPOutputStream stream = new GZIPOutputStream(uploadStreamer)) {
                Semaphore s = new Semaphore(0);
                for (int t = 0; t < genThreads; ++t) {
                    new Thread(() -> {
                        for (int i = 0; i < guidPerThread; ++i) {
                            try {
                                stream.write(nextCSVRow().getBytes());
//                                stream.write(LINE_SEPARATOR);
                            } catch (IOException e) {
                            }
                        }
                        s.release();
                    }).start();
                }
                s.acquire(genThreads);
            }
        }
    }

    private static void uploadTestFileEasyVersion(String bucketName, String file, AmazonS3Client s3client, int guidPerThread) throws IOException, InterruptedException {
        try (S3UploadStreamer uploadStreamer = new S3UploadStreamer(s3client, bucketName, file + EXTENSION_GZ, 1)) {
            try (GZIPOutputStream stream = new GZIPOutputStream(uploadStreamer)) {
                try {
                    for (int i = 0; i < guidPerThread; ++i) {
                        stream.write(nextCSVRow().getBytes());
                    }
                } catch (IOException e) {
                }
            }
        }
    }

    private static void uploadTestFileReallyEasyVersion(String bucketName, String file, AmazonS3Client s3client, int guidPerThread) throws IOException, InterruptedException {
        File tempFile = File.createTempFile("rfm_customer_segmentation_" + 1234 + "_", ".tmp" + EXTENSION_GZ);
        try (GZIPOutputStream bufferedWriter = new GZIPOutputStream(new FileOutputStream(tempFile, true))) {
            bufferedWriter.write(("\'segment\';\'customer_id\';\'basePaymentValue\'" + LINE_SEPARATOR).getBytes());

            for (int i = 0; i < guidPerThread; ++i) {
                bufferedWriter.write(nextCSVRow().getBytes());
            }

        } finally {
            PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, FILE + EXTENSION_GZ, tempFile);
            s3client.putObject(request);

            if (tempFile.exists()) {
                tempFile.delete();
            }
        }
    }


    private static String nextCSVRow() {
        Random r = new Random();

        int segment = (r.nextInt(4) + 1) * 100 + (r.nextInt(4) + 1) * 10 + (r.nextInt(4) + 1);
        long minCustmerId = 1234567L, maxCustomerId = 23456789L;
        long recencyByCustomerId = minCustmerId + ((long) (r.nextDouble() * (maxCustomerId - minCustmerId)));
        BigDecimal min = BigDecimal.ONE, max = new BigDecimal("9999.99");
        BigDecimal monetary = min.add(new BigDecimal(Math.random()).multiply(max.subtract(min))).setScale(2, BigDecimal.ROUND_HALF_UP);

        return "\'" + segment + "\';\'" + recencyByCustomerId + "\';\'" + monetary + "\'" + LINE_SEPARATOR;
    }

    private static HashMap<Integer, HashMap<Long, BigDecimal>> getFromS3(String bucketName, String file, AmazonS3Client s3client) throws IOException {
        HashMap<Integer, HashMap<Long, BigDecimal>> customerIdsAndValuesByRfmSegment = new HashMap<>();

        try (S3Object object = s3client.getObject(new GetObjectRequest(bucketName, file + EXTENSION_GZ));
             InputStream input = object.getObjectContent();) {

            Scanner scanner = new Scanner(new GZIPInputStream(input));
            scanner.nextLine(); // skip first line

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] s = line.replace("\'", "").split(";");
                Preconditions.checkArgument(s.length == 3, "! line in CSV cannot be parsed to proper number of variables, line=\'" + line + "\'");
                int segment = Integer.parseInt(s[0]);
                long customerId = Long.parseLong(s[1]);
                BigDecimal basePaymentValue = new BigDecimal(s[2]);

                if (customerIdsAndValuesByRfmSegment.containsKey(segment)) {
                    HashMap<Long, BigDecimal> customerValueById = customerIdsAndValuesByRfmSegment.get(segment);
                    customerValueById.put(customerId, basePaymentValue);
                } else {
                    HashMap<Long, BigDecimal> customerValueById = new HashMap<>();
                    customerValueById.put(customerId, basePaymentValue);
                    customerIdsAndValuesByRfmSegment.put(segment, customerValueById);
                }
            }
        }

        return customerIdsAndValuesByRfmSegment;
    }
}