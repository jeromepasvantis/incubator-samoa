package org.apache.samoa.test_tasks;

import com.github.javacliparser.*;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.*;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.PrequentialSourceProcessor;
import org.apache.samoa.streams.generators.RandomTreeGenerator;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PalindromeTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;

    private static Logger logger = LoggerFactory.getLogger(org.apache.samoa.tasks.PrequentialEvaluation.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Classifier to train.", Learner.class,
            VerticalHoeffdingTree.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's', "Stream to learn from.",
            InstanceStream.class,
            RandomTreeGenerator.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "Classification performance evaluation method.",
            PerformanceEvaluator.class, BasicClassificationPerformanceEvaluator.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to test/train on  (-1 = no limit).", 1000000, -1,
            Integer.MAX_VALUE);

    public IntOption timeLimitOption = new IntOption("timeLimit", 't',
            "Maximum number of seconds to test/train for (-1 = no limit).", -1, -1,
            Integer.MAX_VALUE);

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f',
            "How many instances between samples of the learning performance.", 100000,
            0, Integer.MAX_VALUE);

    // The frequency of saving model output e.g. predicted class and votes made for individual classes to a file
    // The name of the actual file to which model output will be saved is defined through resultFileOption
    public IntOption labelSampleFrequencyOption = new IntOption("labelSampleFrequency", 'h',
            "How many instances between samples of predicted labels and votes.", 1,
            0, Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n', "Identifier of the evaluation",
            "Prequential_"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    public FileOption dumpFileOption = new FileOption("dumpFile", 'd', "File to append intermediate csv results to",
            null, "csv", true);

    // The name of the CSV file in which model output (and in the case of classification also votes for individual classes)
    // will be saved
    public FileOption resultFileOption = new FileOption("resultFile", 'g', "File to append intermediate model output to",
            null, "csv", true);

    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w',
            "How many microseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);
    // Batch size to delay the incoming stream: delay of x milliseconds after each
    // batch
    public IntOption batchDelayOption = new IntOption("delayBatchSize", 'b',
            "The delay batch size: delay of x milliseconds after each batch ", 1, 1, Integer.MAX_VALUE);

    public FileOption latencyInFileOption = new FileOption("latencyInFileOption", 'x',
            "File to append latency stats for incoming instances",
            null, "csv", true);

    public FileOption latencyOutFileOption = new FileOption("latencyOutFileOption", 'y',
            "File to append latency stats for finished instances",
            null, "csv", true);

    protected PalindromeSourceProcessor palSource;


    protected Stream sourcePiOutputStream;

    private Learner classifier;

    private EvaluatorProcessor evaluator;


    protected Topology palindromeTopology;

    protected TopologyBuilder builder;

    public void getDescription(StringBuilder sb, int indent) {
        sb.append("Palindrome Tak");
    }

    @Override
    public void init() {

        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
        }

        palSource = new PalindromeSourceProcessor();

        builder.addEntranceProcessor(palSource);
        logger.debug("Successfully instantiating PalindromeSourceProcessor");

        Stream streamSourceToFilter = builder.createStream(palSource);

        Processor palFilter = new Processor() {
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
        };
        builder.addProcessor(palFilter, parallelismHintOption.getValue());
        builder.connectInputShuffleStream(sourcePiOutputStream, classifier.getInputProcessor());
        logger.debug("Successfully instantiating Classifier");

        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!org.apache.samoa.tasks.PrequentialEvaluation.isLearnerAndEvaluatorCompatible(classifier, evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(classifier);
        }
        evaluator = new EvaluatorProcessor.Builder(evaluatorOptionValue)
                .samplingFrequency(sampleFrequencyOption.getValue()).dumpFile(dumpFileOption.getFile())
                .predictionFile(resultFileOption.getFile()).latencyFile(latencyOutFileOption.getFile())
                .labelSamplingFrequency(labelSampleFrequencyOption.getValue()).getArgsStringWorkaround(biuldArgStringWorkaround())
                .build();

        // evaluatorPi = builder.createPi(evaluator);
        // evaluatorPi.connectInputShuffleStream(evaluatorPiInputStream);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : classifier.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }

        logger.debug("Successfully instantiating EvaluatorProcessor");

        prequentialTopology = builder.build();
        logger.debug("Successfully building the topology");
    }



    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init()
        // for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        logger.debug("Successfully instantiating TopologyBuilder");

        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());

    }

    public Topology getTopology() {
        return prequentialTopology;
    }

    //
    // @Override
    // public TopologyStarter getTopologyStarter() {
    // return this.preqStarter;
    // }

    protected static boolean isLearnerAndEvaluatorCompatible(Learner learner, PerformanceEvaluator evaluator) {
        return (learner instanceof RegressionLearner && evaluator instanceof RegressionPerformanceEvaluator) ||
                (learner instanceof ClassificationLearner && evaluator instanceof ClassificationPerformanceEvaluator);
    }

    protected static PerformanceEvaluator getDefaultPerformanceEvaluatorForLearner(Learner learner) {
        if (learner instanceof RegressionLearner) {
            return new BasicRegressionPerformanceEvaluator();
        }
        // Default to BasicClassificationPerformanceEvaluator for all other cases
        return new BasicClassificationPerformanceEvaluator();
    }

    private String biuldArgStringWorkaround() {
        StringBuilder argString = new StringBuilder();
        argString.append("[learner: " + learnerOption.getValueAsCLIString() + "] ")
                .append("[data: " + streamTrainOption.getValueAsCLIString()+ "] ")
                .append("[resultFile: " + resultFileOption.getValueAsCLIString()+ "] ")
                .append("[latencyInFile: " + latencyInFileOption.getValueAsCLIString()+ "] ")
                .append("[latencyOutFile: " + latencyOutFileOption.getValueAsCLIString()+ "] ");
        return argString.toString();
    }
}
