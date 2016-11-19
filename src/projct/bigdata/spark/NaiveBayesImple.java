package projct.bigdata.spark;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class NaiveBayesImple {

	public static void main(String[] args) {
		SparkContext sc = new SparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		String path = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
		JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
		JavaRDD<LabeledPoint> training = tmp[0]; // training set
		JavaRDD<LabeledPoint> test = tmp[1]; // test set
		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
		JavaPairRDD<Double, Double> predictionAndLabel =
		  test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		    @Override
		    public Tuple2<Double, Double> call(LabeledPoint p) {
		      return new Tuple2<>(model.predict(p.features()), p.label());
		    }
		  });
		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		  @Override
		  public Boolean call(Tuple2<Double, Double> pl) {
		    return pl._1().equals(pl._2());
		  }
		}).count() / (double) test.count();

		// Save and load model
		model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
		NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "target/tmp/myNaiveBayesModel");
	}
}
