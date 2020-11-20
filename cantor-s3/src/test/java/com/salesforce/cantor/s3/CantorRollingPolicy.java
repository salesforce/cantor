package com.salesforce.cantor.s3;

import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CantorRollingPolicy<E> extends TimeBasedRollingPolicy<E> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void rollover() throws RolloverFailure {
        logger.info("XXXXXXX rollover is called; what is happening!");
        logger.info("filename is: " + getActiveFileName());

        logger.info("going read through the file and spit it out: ");
        try (final BufferedReader reader = new BufferedReader(new FileReader(getActiveFileName()))) {
            while (reader.ready()) {
                logger.info("{}", reader.readLine());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        super.rollover();

        logger.info("now filename is: " + getActiveFileName());
        logger.info("thread name is: " + Thread.currentThread().getName());

        logger.info("going read through the file and spit it out: ");
        try (final BufferedReader reader = new BufferedReader(new FileReader(getActiveFileName()))) {
            while (reader.ready()) {
                logger.info("{}", reader.readLine());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
