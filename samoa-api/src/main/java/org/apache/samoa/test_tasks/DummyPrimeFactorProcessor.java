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
import org.apache.samoa.topology.Stream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tobiasmuench on 06.08.18.
 */
public class DummyPrimeFactorProcessor implements Processor {
    private Stream resultStream;
    @Override
    public boolean process(ContentEvent event) {
        DummyTaskPrimeFactor.IntContentEvent incoming = (DummyTaskPrimeFactor.IntContentEvent) event;
        int n = incoming.number;
        List<Integer> factors = new ArrayList<Integer>();
        for (int i = 2; i <= n; i++) {
            while (n % i == 0) {
                factors.add(i);
                n /= i;
            }
        }
        ContentEvent result = new DummyTaskPrimeFactor.FactorContentEvent(incoming.number, factors, incoming.timestamp);
        resultStream.put(result);
        return false;
    }

    @Override
    public void onCreate(int id) {

    }

    @Override
    public Processor newProcessor(Processor processor) {
        DummyPrimeFactorProcessor newP = new DummyPrimeFactorProcessor();
        newP.setResultStream(((DummyPrimeFactorProcessor) processor).resultStream);
        return newP;
    }

    public void setResultStream(Stream resultStream) {
        this.resultStream = resultStream;
    }
}
