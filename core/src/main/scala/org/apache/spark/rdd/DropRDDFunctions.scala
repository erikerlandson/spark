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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, Logging, Partition, TaskContext}
import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency}

// Assuming child RDD type having only one partition
class FanOutDep[T: ClassTag](rdd: RDD[T])
  extends NarrowDependency[T](rdd) {
  override def getParents(pid: Int) = (0 until rdd.partitions.length)
}

// Assuming parent RDD type having only one partition
// Arguably, this is not technically a narrow dep because it is one to many
// However it is not a shuffle dep, as it does not involve any changes to
// data partitioning, or movement of data between partitions.
class FanInDep[T: ClassTag](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(pid: Int) = List(0)
}

// A PromiseRDD has exactly one partition, by construction:
class PromisePartition extends Partition {
  override def index = 0
}

// A way to represent the concept of a promised expression as an RDD, so that it
// can operate inside the lazy-transform formalism
class PromiseRDD[V: ClassTag](expr: TaskContext => V, context: SparkContext, deps: Seq[Dependency[_]])
  extends RDD[V](context, deps) {

  // This RDD has exactly one partition by definition, since it will contain
  // a single row holding the 'promised' result of evaluating 'expr' 
  override def getPartitions = Array(new PromisePartition)

  // compute evaluates 'expr', yielding an iterator over a sequence of length 1:
  override def compute(p: Partition, ctx: TaskContext) = List(expr(ctx)).iterator
}

class DropPartition[V: ClassTag](ppart: Partition, locRDD: PromiseRDD[V]) extends Partition {
  override def index = ppart.index
  def pp = ppart
  def loc(ctx: TaskContext): V = locRDD.iterator(new PromisePartition, ctx).next
}

/**
 * Extra functions available on RDDs for providing the RDD analogs of Scala drop,
 * dropRight and dropWhile, which return an RDD as a result
 */
class DropRDDFunctions[T : ClassTag](self: RDD[T]) extends Logging with Serializable {

  /**
   * Return a PromiseRDD by applying function 'f' to the partitions of this RDD
   */
  def promiseFromPartitions[V: ClassTag](f: Seq[Iterator[T]] => V): PromiseRDD[V] = {
    val expr = (ctx: TaskContext) => f(self.partitions.map(s => self.iterator(s, ctx)))
    new PromiseRDD[V](expr, self.context, List(new FanOutDep(self))) 
  }

  def promiseFromPartitionArray[V: ClassTag](f: (Array[Partition], RDD[T], TaskContext) => V): PromiseRDD[V] = {
    val expr = (ctx: TaskContext) => f(self.partitions, self, ctx)
    new PromiseRDD[V](expr, self.context, List(new FanOutDep(self))) 
  }


/**
 * Return a new RDD formed by dropping the first (n) elements of the input RDD
 */
  def drop(n: Int):RDD[T] = {
    if (n <= 0) return self

    // locate partition that includes the nth element
    val locate = (partitions: Array[Partition], parent: RDD[T], ctx: TaskContext) => {
      var rem = n
      var p = 0
      var np = 0
      while (rem > 0  &&  p < partitions.length) {
        np = parent.iterator(partitions(p), ctx).length
        rem -= np
        p += 1
      }

      if (rem > 0  ||  (rem == 0  &&  p >= partitions.length)) {
        // all elements were dropped
        (p, 0)
      } else {
        // (if we get here, note that rem <= 0)
        (p - 1, np + rem)
      }
    }

    val locRDD = this.promiseFromPartitionArray(locate)

    new RDD[T](self.context, List(new OneToOneDependency(self), new FanInDep(locRDD))) {
      override def getPartitions: Array[Partition] = self.partitions.map(p => new DropPartition(p, locRDD))
      override val partitioner = self.partitioner
      override def compute(split: Partition, ctx: TaskContext):Iterator[T] = {
        val dp = split.asInstanceOf[DropPartition[(Int, Int)]]
        val (pFirst, pDrop) = dp.loc(ctx)
        val parent = firstParent[T]
        if (dp.index > pFirst) return parent.iterator(dp.pp, ctx)
        if (dp.index == pFirst) return parent.iterator(dp.pp, ctx).drop(pDrop)
        Iterator.empty
      }
    }
  }


/**
 * Return a new RDD formed by dropping the last (n) elements of the input RDD
 */
  def dropRight(n: Int):RDD[T] = {
    if (n <= 0) return self

    // locate partition that includes the nth element
    var rem = n
    var p = self.partitions.length-1
    var np = 0
    while (rem > 0  &&  p >= 0) {
      val res = self.sparkContext.runJob(self, (it: Iterator[T]) => it.length,
                                         p until 1 + p, allowLocal = true)
      np = res(0)
      rem -= np
      p -= 1
    }

    // all elements were dropped
    if (rem > 0  ||  (rem == 0  &&  p < 0)) {
      return self.sparkContext.emptyRDD[T]
    }

    // Return an RDD that discounts the last (n) elements of the parent RDD
    // (if we get here, note that rem <= 0)
    val pFirst = p + 1
    val pTake = -rem
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index < pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).take(pTake)
        Iterator.empty
      }
    }
  }  


/**
 * Return a new RDD formed by dropping leading elements until predicate function (f) returns false
 */
  def dropWhile(f: T=>Boolean):RDD[T] = {
    var p = 0
    var np = 0
    while (np <= 0  &&  p < self.partitions.length) {
      val res = self.sparkContext.runJob(self, (it: Iterator[T]) => it.dropWhile(f).length,
                                         p until 1 + p, allowLocal = true)
      np = res(0)
      p += 1
    }

    // all elements were dropped
    if (np <= 0  &&  p >= self.partitions.length) {
      return self.sparkContext.emptyRDD[T]
    }

    val pFirst = p - 1
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index > pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).dropWhile(f)
        Iterator.empty
      }
    }    
  }


}
