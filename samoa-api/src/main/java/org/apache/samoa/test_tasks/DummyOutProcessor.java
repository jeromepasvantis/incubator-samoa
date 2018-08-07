package org.apache.samoa.test_tasks;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2018 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by tobiasmuench on 06.08.18.
 */
public class DummyOutProcessor implements Processor{

    int numEvents;
    File statFile;

    private static Logger logger = LoggerFactory.getLogger(org.apache.samoa.test_tasks.DummyOutProcessor.class);


    PrintStream outputStream;

    DummyOutProcessor(File file) {
        this.statFile = file;
    }
    @Override
    public boolean process(ContentEvent event) {
        DummyTaskPrimeFactor.FactorContentEvent incoming = (DummyTaskPrimeFactor.FactorContentEvent) event;
        long timeInPipeline = TimeUnit.MILLISECONDS.convert(System.nanoTime() - incoming.timestamp, TimeUnit.NANOSECONDS);
        outputStream.println(numEvents + "," + incoming.number + "," + timeInPipeline);
        logger.info("{" + numEvents + "} "
                + incoming.number
                + " -> " + Arrays.toString(incoming.factors.toArray())
                + "(duration (ms): "
                + timeInPipeline
                + ") ");
        numEvents++;
        return true;
    }

    @Override
    public void onCreate(int id) {
        this.numEvents = 0;
        if (this.statFile != null) {
            try {
                if (statFile.exists()) {
                    this.outputStream = new PrintStream(
                            new FileOutputStream(statFile, true), true);
                } else {
                    this.outputStream = new PrintStream(
                            new FileOutputStream(statFile), true);
                }

            } catch (FileNotFoundException e) {
                this.outputStream = null;
                //logger.error("File not found exception for {}:{}", this.latencyStatFile.getAbsolutePath(), e.toString());

            } catch (Exception e) {
                this.outputStream = null;
                //logger.error("Exception when creating {}:{}", this.latencyStatFile.getAbsolutePath(), e.toString());
            }
            outputStream.println("START TIME: " + LocalDateTime.now());
            outputStream.println("Event Number, " + "Number to Factor, " + "duration");

        }
    }

    @Override
    public Processor newProcessor(Processor processor) {
        return new DummyOutProcessor(this.statFile);
    }
}
