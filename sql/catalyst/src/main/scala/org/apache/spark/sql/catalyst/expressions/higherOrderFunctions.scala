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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

object HigherOrderFunction {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  def generateVariable(dataType: DataType, nullable: Boolean): LambdaVariable = {
    val id = curId.getAndIncrement()
    val loopValue = "loopValue" + id
    val loopIsNull = "loopIsNull" + id
    LambdaVariable(loopValue, loopIsNull, dataType, nullable)
  }
}

trait HigherOrderFunction extends Expression {
  override def children: Seq[Expression] = (inputs :+ function) ++ variables

  /**
   * Inputs to the higher ordered function.
   */
  def inputs: Seq[Expression]

  /**
   * All inputs have been resolved. This means that the variables should be known, and that we can
   * start binding the function.
   */
  lazy val inputResolved: Boolean = inputs.forall(_.resolved)

  /**
   * Variables to bind to the function.
   */
  def variables: Seq[LambdaVariable]

  /**
   * Function to call for part of the processing, this can either be a regular function or a
   * lambda function.
   */
  def function: Expression

  /**
   * Bind Lambda variables for
   * @return
   */
  def bindVariables(): HigherOrderFunction

  /**
   * Bind the higher order function to a new (hopefully resolved) function. Note that we only
   * should attempt to bind a function after the inputs have been resolved and variables have
   * been created.
   */
  def bindFunction(function: Expression): HigherOrderFunction
}

trait ArrayBasedHigherOrderFunction extends HigherOrderFunction with ExpectsInputTypes {
  def input: Expression

  override def inputs: Seq[Expression] = input :: Nil

  lazy val variable: LambdaVariable = variables.head

  override lazy val inputResolved: Boolean = {
    input.resolved && ArrayType.acceptsType(input.dataType)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, AnyDataType, AnyDataType)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  protected def newInstance(
      variables: Seq[LambdaVariable] = variables,
      function: Expression = function): ArrayBasedHigherOrderFunction

  override def bindVariables(): ArrayBasedHigherOrderFunction = {
    val ArrayType(elementType, containsNull) = input.dataType
    newInstance(variables = Seq(HigherOrderFunction.generateVariable(elementType, containsNull)))
  }

  override def bindFunction(function: Expression): ArrayBasedHigherOrderFunction = {
    newInstance(function = function)
  }
}

/**
 * Transform elements in an array using the transform function. This is similar to
 * a `map` in functional programming.
 */
case class ArrayTransform(
    input: Expression,
    function: Expression,
    variables: Seq[LambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = input.nullable

  override def dataType: DataType = ArrayType(function.dataType, function.nullable)

  override protected def newInstance(
      variables: Seq[LambdaVariable],
      function: Expression): ArrayBasedHigherOrderFunction = {
    ArrayTransform(input, function, variables)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Get the array
    val childGen = input.genCode(ctx)
    val arr = childGen.value

    // iterate over the array
    val i = ctx.freshName("i")
    val length = ctx.freshName("numElements")
    val target = ctx.freshName("target")
    val fEval = function.genCode(ctx)
    ev.copy(code = s"""
       |${childGen.code}
       |boolean ${ev.isNull} = true;
       |${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
       |if (!${childGen.isNull}) {
       |  int $length = $arr.numElements();
       |  Object[] $target = new Object[$length];
       |  ${ctx.javaType(variable.dataType)} ${variable.value};
       |  boolean ${variable.isNull};
       |  for (int $i = 0; $i < $length; $i++) {
       |    // Load the lambda variable.
       |    ${variable.isNull} = $arr.isNullAt($i);
       |    if (${variable.isNull}) {
       |      ${variable.value} = ${ctx.defaultValue(variable.dataType)};
       |    } else {
       |      ${variable.value} = ${ctx.getValue(arr, variable.dataType, i)};
       |    }
       |
       |    // Invoke the function
       |    ${fEval.code}
       |
       |    // Copy the result to the target array
       |    if (!${fEval.isNull}) {
       |      $target[$i] = ${fEval.value};
       |    }
       |  }
       |  ${ev.isNull} = false;
       |  ${ev.value} = new ${classOf[GenericArrayData].getName}($target);
       |}
     """.stripMargin)
  }
}

/**
 * Test if a predicate holds for at least one element in the array.
 */
case class ArrayExists(
    input: Expression,
    function: Expression,
    variables: Seq[LambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = false

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType, AnyDataType)

  override protected def newInstance(
      variables: Seq[LambdaVariable],
      function: Expression): ArrayBasedHigherOrderFunction = {
    ArrayExists(input, function, variables)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Get the array
    val childGen = input.genCode(ctx)
    val arr = childGen.value

    // iterate over the array
    val i = ctx.freshName("i")
    val length = ctx.freshName("numElements")
    val fEval = function.genCode(ctx)
    ev.copy(isNull = "false", code = s"""
       |${childGen.code}
       |boolean ${ev.value} = false;
       |if (!${childGen.isNull}) {
       |  int $length = $arr.numElements();
       |  ${ctx.javaType(variable.dataType)} ${variable.value};
       |  boolean ${variable.isNull};
       |  for (int $i = 0; $i < $length && !${ev.value}; $i++) {
       |    // Load the lambda variable.
       |    ${variable.isNull} = $arr.isNullAt($i);
       |    if (${variable.isNull}) {
       |      ${variable.value} = ${ctx.defaultValue(variable.dataType)};
       |    } else {
       |      ${variable.value} = ${ctx.getValue(arr, variable.dataType, i)};
       |    }
       |
       |    // Invoke the function
       |    ${fEval.code}
       |
       |    // Update the value.
       |    ${ev.value} = !${fEval.isNull} && ${fEval.value};
       |  }
       |}
     """.stripMargin)
  }
}

/**
 * Filter array elements using a predicate.
 */
case class ArrayFilter(
    input: Expression,
    function: Expression,
    variables: Seq[LambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = false

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType, AnyDataType)

  override protected def newInstance(
      variables: Seq[LambdaVariable],
      function: Expression): ArrayBasedHigherOrderFunction = {
    ArrayFilter(input, function, variables)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Get the array
    val childGen = input.genCode(ctx)
    val arr = childGen.value

    // iterate over the array
    val i = ctx.freshName("i")
    val length = ctx.freshName("numElements")
    val target = ctx.freshName("target")
    val numTargetElements = ctx.freshName("numTargetElements")
    val fEval = function.genCode(ctx)
    ev.copy(code = s"""
       |${childGen.code}
       |boolean ${ev.isNull} = true;
       |${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
       |if (!${childGen.isNull}) {
       |  int $length = $arr.numElements();
       |  int $numTargetElements = 0;
       |  Object[] $target = new Object[$length];
       |  ${ctx.javaType(variable.dataType)} ${variable.value};
       |  boolean ${variable.isNull};
       |  for (int $i = 0; $i < $length; $i++) {
       |    // Load the lambda variable.
       |    ${variable.isNull} = $arr.isNullAt($i);
       |    if (${variable.isNull}) {
       |      ${variable.value} = ${ctx.defaultValue(variable.dataType)};
       |    } else {
       |      ${variable.value} = ${ctx.getValue(arr, variable.dataType, i)};
       |    }
       |
       |    // Invoke the filter function
       |    ${fEval.code}
       |
       |    // Copy the element to the target array
       |    if (!${fEval.isNull} && ${fEval.value}) {
       |      if (!${variable.isNull}) {
       |        $target[$numTargetElements] = ${variable.value};
       |      }
       |      $numTargetElements++;
       |    }
       |  }
       |  ${ev.isNull} = false;
       |  $target = java.util.Arrays.copyOf($target, $numTargetElements);
       |  ${ev.value} = new ${classOf[GenericArrayData].getName}($target);
       |}
     """.stripMargin)
  }
}

case class ArrayReduce(
    input: Expression,
    zero: Expression,
    function: Expression,
    variables: Seq[LambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, zero: Expression, function: Expression) = {
    this(input, zero, function, Nil)
  }

  override def inputs: Seq[Expression] = input :: zero :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = zero.dataType

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(ArrayType, AnyDataType, zero.dataType, AnyDataType, AnyDataType)
  }

  override protected def newInstance(
      variables: Seq[LambdaVariable],
      function: Expression): ArrayBasedHigherOrderFunction = {
    ArrayReduce(input, zero, function, variables)
  }

  override def bindVariables(): ArrayBasedHigherOrderFunction = {
    val ArrayType(elementType, containsNull) = input.dataType
    newInstance(variables = Seq(
      HigherOrderFunction.generateVariable(elementType, containsNull),
      HigherOrderFunction.generateVariable(zero.dataType, containsNull || zero.nullable)))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Get the array
    val childGen = input.genCode(ctx)
    val arr = childGen.value

    // Get zero
    val zeroGen = zero.genCode(ctx)

    // iterate over the array
    val acc = variables(1)
    val i = ctx.freshName("i")
    val length = ctx.freshName("numElements")
    val fEval = function.genCode(ctx)
    val code = s"""
    |${childGen.code}
    |${ctx.javaType(dataType)} ${acc.value} = ${zeroGen.value};
    |boolean ${acc.isNull} = ${zeroGen.isNull};
    |if (!${childGen.isNull}) {
    |  int $length = $arr.numElements();
    |  ${ctx.javaType(variable.dataType)} ${variable.value};
    |  boolean ${variable.isNull};
    |  for (int $i = 0; $i < $length; $i++) {
    |    // Load the lambda variable.
    |    ${variable.isNull} = $arr.isNullAt($i);
    |    if (${variable.isNull}) {
    |      ${variable.value} = ${ctx.defaultValue(variable.dataType)};
    |    } else {
    |      ${variable.value} = ${ctx.getValue(arr, variable.dataType, i)};
    |    }
    |
    |    // Invoke the function
    |    ${fEval.code}
    |
    |    // Assign the new accumulator value.
    |    ${acc.isNull} = ${fEval.isNull};
    |    ${acc.value} = ${fEval.value};
    |  }
    |}
    |""".stripMargin
    ExprCode(code, acc.isNull, acc.value)
  }
}
