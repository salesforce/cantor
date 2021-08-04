package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Cantor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Test {

    public static void main(String args[]) throws IOException {
        final Cantor cantor = new CantorOnGrpc("cantor.casp.prd-samtwo.prd.slb.sfdc.net:11983");
//        cantor.objects().drop("test");
//        cantor.objects().create("test");
        System.out.println(cantor.objects().size("test"));
        for (int i = 0; i < 100_000; ++i) {
            try {
                long start = System.currentTimeMillis();
                System.out.println("size: " + cantor.objects().size("configs"));
                cantor.objects().get("test", "foo");
                long end = System.currentTimeMillis();
                System.out.println(end - start);
            } catch (Exception e) {
                System.out.println("iteration " + i);
                e.printStackTrace();
            }
        }
        System.out.println(cantor.objects().size("test"));
        cantor.objects().drop("test");
    }
}
