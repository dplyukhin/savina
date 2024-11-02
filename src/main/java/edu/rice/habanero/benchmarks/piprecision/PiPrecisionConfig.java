package edu.rice.habanero.benchmarks.piprecision;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class PiPrecisionConfig {

    private static final BigDecimal one = BigDecimal.ONE;
    private static final BigDecimal two = new BigDecimal(2);
    private static final BigDecimal four = new BigDecimal(4);
    private static final BigDecimal sixteen = new BigDecimal(16);
    protected static int NUM_WORKERS = 20;
    protected static int PRECISION = 5_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-w":
                    i += 1;
                    NUM_WORKERS = Integer.parseInt(args[i]);
                    break;
                case "-p":
                    i += 1;
                    PRECISION = Integer.parseInt(args[i]);
                    break;
                case "debug":
                case "verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Workers", NUM_WORKERS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Precision", PRECISION);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    /**
     * Original Source Code: http://research.cs.queensu.ca/home/cmpe212/Fall2011/Lab6/Lab6.java Formula:
     * http://mathworld.wolfram.com/BBPFormula.html
     */
    protected static BigDecimal calculateBbpTerm(final int scale, final int k) {
        final RoundingMode roundMode = RoundingMode.HALF_EVEN;

        final int eightK = 8 * k;
        BigDecimal term = four.divide(new BigDecimal(eightK + 1), scale, roundMode);
        term = term.subtract(two.divide(new BigDecimal(eightK + 4), scale, roundMode));
        term = term.subtract(one.divide(new BigDecimal(eightK + 5), scale, roundMode));
        term = term.subtract(one.divide(new BigDecimal(eightK + 6), scale, roundMode));
        term = term.divide(sixteen.pow(k), scale, roundMode);
        return term;
    }
}
