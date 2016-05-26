object demo {
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.DenseVector
  import org.apache.spark.mllib.tree.RandomForest
  import org.apache.spark.mllib.tree.configuration.Strategy

  def generateData = {
    val t1 = Vector.tabulate(10) { t =>
      Vector.fill(50) {
        new LabeledPoint(
          1.0,
          new DenseVector(Array.tabulate(10) { j => if (j == t) 1.0 else 0.0 }))
      }
    }.foldLeft(Vector.empty[LabeledPoint]) { case (a, b) => a ++ b }
    val t2 = Vector.fill(500) {
      new LabeledPoint(
        0.0,
        new DenseVector(Array.fill(10) { 0.0 }))
    }
    t1 ++ t2
  }

  lazy val data = sc.parallelize(generateData)

  lazy val catInfo = Map(Vector.tabulate(10) { j => (j, 2) } :_*)

  def toGain(pval: Double) = -math.log(pval)

  def train(measure: String, minGain: Double) = {
    val imp = measure match {
      case "gini" => org.apache.spark.mllib.tree.impurity.Gini
      case "entropy" => org.apache.spark.mllib.tree.impurity.Entropy
      case "chisquared" => org.apache.spark.mllib.tree.impurity.ChiSquared
      case _ => throw new Exception("noooooo!")
    }
    val strategy = Strategy.defaultStrategy("classification")
    strategy.minInfoGain = minGain
    strategy.categoricalFeaturesInfo = catInfo
    strategy.impurity = imp
    strategy.numClasses = 2
    strategy.maxDepth = 12
    strategy.maxBins = 2
    strategy.minInstancesPerNode = 10

    RandomForest.trainClassifier(
      data,
      strategy,
      1,
      "auto",
      73)
  }
}
