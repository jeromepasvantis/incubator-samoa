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
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketPermission;


/**
 * Created by tobiasmuench on 06.08.18.
 */
public class DummyEntranceProcessor implements EntranceProcessor{

    int maxIter;
    int numberToFactor;
    int currentIter;

    DummyEntranceProcessor(int numNumbers, int numberToFactor){
        this.maxIter = numNumbers;
        this.currentIter = 0;
        this.numberToFactor = numberToFactor;
    }
    @Override
    public void onCreate(int id) {
        currentIter = 0;
    }

    @Override
    public boolean isFinished() {
        return !hasNext();
    }

    @Override
    public boolean hasNext() {
        return currentIter < maxIter;
    }

    @Override
    public ContentEvent nextEvent() {
        long timeStamp = System.nanoTime();
        if (hasNext()) {
            currentIter++;
            return new DummyTaskPrimeFactor.IntContentEvent(numberToFactor, timeStamp);
        }
        else {
            DummyTaskPrimeFactor.IntContentEvent lastEvent = new DummyTaskPrimeFactor.IntContentEvent(numberToFactor, timeStamp);
            lastEvent.isLastEvent = true;
            return lastEvent;
        }
    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        return new DummyEntranceProcessor(this.maxIter, this.currentIter);
    }
}
