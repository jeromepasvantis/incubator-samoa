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

import com.github.javacliparser.*;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public class DummyTaskPrimeFactor implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;

    private static Logger logger = LoggerFactory.getLogger(org.apache.samoa.test_tasks.DummyTaskPrimeFactor.class);


    public IntOption parallelismHintOption = new IntOption("parallelism", 'p',
            "parallelism", 1,
            1, Integer.MAX_VALUE);

    public IntOption numNumbers = new IntOption("maxIterations", 'n',
            "How many rounds", 10000,
            1, Integer.MAX_VALUE-10000);

    public IntOption numberToFactor = new IntOption("numberToFactor", 's',
            "Number to check for Prime Factors", 999777,
            1, Integer.MAX_VALUE);
    public IntOption sampleFrequency = new IntOption("sampleFrequency", 'f',
            "How often to sample Stats", 100,
            1, Integer.MAX_VALUE);

    public FileOption statFile = new FileOption("statFile", 'd', "File to append statistics to",
            null, "csv", true);


    protected Topology dummyTopology;

    protected TopologyBuilder builder;
    String topologyName = "DummyTask";

    public void getDescription(StringBuilder sb, int indent) {
        sb.append("Dummy Tak");
    }

    @Override
    public void init() {

        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(topologyName);
            logger.debug("Successfully initializing SAMOA topology with name {}", topologyName);
        }

        DummyEntranceProcessor generatorProc = new DummyEntranceProcessor(numNumbers.getValue(), numberToFactor.getValue());

        builder.addEntranceProcessor(generatorProc);
        logger.debug("Successfully instantiating GeneratorProcessor");

        Stream streamSourceToWorker = builder.createStream(generatorProc);

        DummyPrimeFactorProcessor pfProcessor = new DummyPrimeFactorProcessor();

        builder.addProcessor(pfProcessor, parallelismHintOption.getValue());
        builder.connectInputShuffleStream(streamSourceToWorker, pfProcessor);
        logger.debug("Successfully instantiating PrimeFactorProcessor");

        DummyOutProcessor outputProcessor = new DummyOutProcessor(statFile.getFile(), sampleFrequency.getValue());

        builder.addProcessor(outputProcessor);

        Stream streamWorkerToOutput = builder.createStream(pfProcessor);
        pfProcessor.setResultStream(streamWorkerToOutput);
        builder.connectInputShuffleStream(streamWorkerToOutput, outputProcessor);


        logger.debug("Successfully instantiating outputProc");

        dummyTopology = builder.build();
        logger.debug("Successfully building the topology");
    }



    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init()
        // for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        logger.debug("Successfully instantiating TopologyBuilder");

        builder.initTopology(topologyName);
        logger.debug("Successfully initializing SAMOA topology with name {}", topologyName);

    }

    public Topology getTopology() {
        return dummyTopology;
    }


    public static class IntContentEvent implements ContentEvent {

        long timestamp;
        int number;
        boolean isLastEvent;

        public IntContentEvent(int number, long timestamp) {
            this.number = number;
            this.timestamp = timestamp;
        }

        @Override
        public String getKey() {
            return Integer.toString(number);
        }

        @Override
        public void setKey(String key) {
            this.number = Integer.parseInt(key);
        }

        @Override
        public boolean isLastEvent() {
            return isLastEvent;
        }
    }

    public static class FactorContentEvent implements ContentEvent {
        int number;
        List<Integer> factors;
        long timestamp;

        FactorContentEvent(int number, List<Integer> factors, long timestamp) {
            this.number = number;
            this.factors = factors;
            this.timestamp = timestamp;
        }

        @Override
        public String getKey() {
            return Integer.toString(number);
        }

        @Override
        public void setKey(String key) {
            this.number = Integer.parseInt(key);
        }

        @Override
        public boolean isLastEvent() {
            return false;
        }
    }
}
