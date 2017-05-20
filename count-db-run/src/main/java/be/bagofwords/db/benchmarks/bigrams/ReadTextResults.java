package be.bagofwords.db.benchmarks.bigrams;

/**
* Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
*/
class ReadTextResults {
    private double readsPerSecond;
    private double writesPerSecond;

    ReadTextResults(double readsPerSecond, double writesPerSecond) {
        this.readsPerSecond = readsPerSecond;
        this.writesPerSecond = writesPerSecond;
    }

    public double getReadsPerSecond() {
        return readsPerSecond;
    }

    public double getWritesPerSecond() {
        return writesPerSecond;
    }
}
