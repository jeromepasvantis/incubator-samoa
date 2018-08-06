package org.apache.samoa.test_tasks;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;

/**
 * Created by tobiasmuench on 06.08.18.
 */
public class PalindromeSourceProcessor implements EntranceProcessor {
    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public void onCreate(int id) {

    }

    @Override
    public Processor newProcessor(Processor processor) {
        return null;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ContentEvent nextEvent() {
        return null;
    }
}
