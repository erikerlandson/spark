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

import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency}

// Assuming child RDD type having only one partition
class FanOutDep[T: ClassTag](rdd: RDD[T])
  extends NarrowDependency[T](rdd) {
  override def getParents(pid: Int) = (0 until rdd.partitions.length)
}

// Assuming parent RDD type having only one partition
// This is not technically a narrow dep because it is one to many
class FanInDep[T: ClassTag](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(pid: Int) = List(0)
}

class UnitaryPartition extends Partition {
  override def index = 0
}

class DropPartition[V: ClassTag](ppart: Partition, locRDD: RDD[V]) extends Partition {
  override def index = ppart.index
  def pp = ppart
  def loc(ctx: TaskContext): V = locRDD.iterator(new UnitaryPartition, ctx).next
}

/**
 * Extra functions available on RDDs for providing the RDD analogs of Scala drop,
 * dropRight and dropWhile, which return an RDD as a result
 */
class DropRDDFunctions[T : ClassTag](self: RDD[T]) extends Logging with Serializable {

  // A way to represent the concept of applying a function to an RDD to get a single value,
  // inside the lazy transform formalism
  // Returns a specialized RDD that contains a single row which is the value returned
  // from 'f' applied to the partitions of the parent RDD
  // This version works on Iterator[T] and is something you could present to user visible API
  // Note this version forces computation of all parent partitions
  def applyFunctionToPartitions[V: ClassTag](f: Seq[Iterator[T]] => V): RDD[V] = {
    new RDD[V](self.context, List(new FanOutDep(self))) {
      // If there is a way to get list of parent partitions from worker environment, 
      // I could just keep parent without pre-caching the partition array
      val parent:RDD[T] = self
      val plist = parent.partitions

      // this RDD has just one partition, since it only holds a single value in a single row
      override def getPartitions: Array[Partition] = Array(new UnitaryPartition)

      // to compute the single output value, apply the function to the sequence of parent partitions
      override def compute(prt: Partition, ctx: TaskContext): Iterator[V] = {
        Array(f(plist.map(s => parent.iterator(s, ctx)))).iterator
      }
    }
  }

  // This variation is suited for internal use
  // works directly on sequence of Partition structures
  // This version can be specific about which parent partitions it invokes compute on
  def applyFunctionToPartitionArray[V: ClassTag](f: (Array[Partition], RDD[T], TaskContext) => V): RDD[V] = {
    new RDD[V](self.context, List(new FanOutDep(self))) {
      // is it possible to get partition list from worker environment without pre-caching it?
      val parent:RDD[T] = self
      val plist:Array[Partition] = parent.partitions

      // always a single partition, which will contain a single row
      override def getPartitions: Array[Partition] = Array(new UnitaryPartition)

      // generate a single row, containing result of applying 'f' to sequence of parent's partitions
      override def compute(prt: Partition, ctx: TaskContext): Iterator[V] = {
        Array(f(plist, parent, ctx)).iterator
      }
    }
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

      // all elements were dropped
      if (rem > 0  ||  (rem == 0  &&  p >= partitions.length)) {
        (p, 0)
      } else {
        // (if we get here, note that rem <= 0)
        (p - 1, np + rem)
      }
    }

    val locRDD = this.applyFunctionToPartitionArray(locate)

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
