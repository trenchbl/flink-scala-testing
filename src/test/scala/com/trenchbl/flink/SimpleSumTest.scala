package com.trenchbl.flink

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.test.util.AbstractTestBase
import org.junit.runner.RunWith
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

//@RunWith(classOf[JUnitRunner])
class SimpleSumTest extends AbstractTestBase with Serializable {

  @Test
  def scenario01(): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(1)
    //
    //
    //    val sourceOne = env.addSource(new SourceFunction[Integer]() {
    //      override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {
    //        ctx.collectWithTimestamp(1, 100L)
    //        ctx.collectWithTimestamp(3, 300L)
    //      }
    //
    //      override def cancel(): Unit = {}
    //    })
    //
    //    val sourceTwo = env.addSource(new SourceFunction[Integer]() {
    //      override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {
    //        ctx.collectWithTimestamp(2, 200L)
    //        ctx.collectWithTimestamp(5, 500L)
    //      }
    //
    //      override def cancel(): Unit = {}
    //    })
    //
    //    val sinkOutput = mutable.MutableList[Integer]()
    //
    //    sourceOne.keyBy(x => 1).connect(sourceTwo.keyBy(x => 1))
    //      .process(new SimpleSum())
    //      .addSink(new SinkFunction[Integer] {
    //        override def invoke(value: Integer) {
    //          sinkOutput += value
    //        }
    //      })
    //
    //    env.execute("Reduce Window Test")
    //
    //    val expectedResult = mutable.MutableList(1, 4, 6, 11)
    //
    //    Assert.assertEquals(expectedResult, sinkOutput)
    val summingFunction = new SimpleSum
    val testOperator = new KeyedCoProcessOperator[Integer, Integer, Integer, Integer](summingFunction)

    val keySelector = new KeySelector[Integer, Integer] {
      override def getKey(in: Integer): Integer = 1
    }

    val testHarness = new KeyedTwoInputStreamOperatorTestHarness[Integer, Integer, Integer, Integer](testOperator, keySelector,
      keySelector, BasicTypeInfo.INT_TYPE_INFO)

    testHarness.open()

    testHarness.processElement1(new StreamRecord[Integer](1))

    Assert.assertEquals(1, testHarness.getOutput.peek().asInstanceOf[StreamRecord[Integer]].getValue)

    testHarness.close()
  }

//  @SerialVersionUID(1L)
//  object SimpleSumTest {
//    val testResults = mutable.MutableList[Integer]()
//  }

}
