/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.tree.impurity

import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}

object ChiSquared extends Impurity {
  object cht extends org.apache.commons.math3.stat.inference.ChiSquareTest()

  def instance: this.type = this

  override def calculate(counts: Array[Double], totalCount: Double): Double =
    throw new UnsupportedOperationException("ChiSquared.calculate")

  override def calculate(count: Double, sum: Double, sumSquares: Double): Double =
    throw new UnsupportedOperationException("ChiSquared.calculate")

  override def calculate(calcL: ImpurityCalculator, calcR: ImpurityCalculator): Double = {
    cht.chiSquareTest(
      Array(
        calcL.stats.map(_.toLong),
        calcR.stats.map(_.toLong)
      )
    )
  }

  override def isTestStatistic = true
}

private[spark] class ChiSquaredAggregator(numClasses: Int)
  extends ImpurityAggregator(numClasses) with Serializable {

  def update(allStats: Array[Double], offset: Int, label: Double, instanceWeight: Double): Unit = {
    allStats(offset + label.toInt) += instanceWeight
  }

  def getCalculator(allStats: Array[Double], offset: Int): ChiSquaredCalculator = {
    new ChiSquaredCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

private[spark] class ChiSquaredCalculator(stats: Array[Double]) extends ImpurityCalculator(stats) {

  def copy: ChiSquaredCalculator = new ChiSquaredCalculator(stats.clone())

  def calculate(): Double = 1.0

  def count: Long = stats.sum.toLong

  def predict: Double =
    if (count == 0) 0 else indexOfLargestArrayElement(stats)

  override def prob(label: Double): Double = {
    val lbl = label.toInt
    require(lbl < stats.length,
      s"ChiSquaredCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "ChiSquaredImpurity does not support negative labels")
    val cnt = count
    if (cnt == 0) 0 else (stats(lbl) / cnt)
  }

  override def toString: String = s"ChiSquaredCalculator(stats = [${stats.mkString(", ")}])"
}
