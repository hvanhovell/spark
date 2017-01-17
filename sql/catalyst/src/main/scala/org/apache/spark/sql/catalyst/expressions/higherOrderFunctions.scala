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
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * A named lambda variable.
 *
 * Note that the current implementation does not support interpreted execution.
 */
case class NamedLambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    exprId: ExprId = NamedExpression.newExprId)
  extends LeafExpression
  with NamedExpression
  with Unevaluable {

  private val suffix = "_lambda_variable_" + exprId.id

  val value: String = name + suffix

  val isNull: String = "isNull" + suffix

  override def qualifier: Option[String] = None

  override def newInstance(): NamedExpression = copy(exprId = NamedExpression.newExprId)

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, None, true)
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode("", isNull, value)
  }

  override def toString: String = s"lambda $name#${exprId.id}$typeSuffix"

  override def simpleString: String = s"lambda $name#${exprId.id}: ${dataType.simpleString}"
}

/**
 * A higher order function takes a (lambda) function and applies this to a (complex) Spark object.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait HigherOrderFunction extends Expression {
  override def children: Seq[Expression] = (inputs :+ function) ++ variables

  /**
   * Inputs to the higher ordered function.
   */
  def inputs: Seq[Expression]

  /**
   * All inputs have been resolved. This means that the variables should be known, and that we can
   * start binding the lambda function.
   */
  lazy val inputResolved: Boolean = inputs.forall(_.resolved)

  /**
   * Variables produced by the higher order function.
   */
  def variables: Seq[NamedLambdaVariable]

  /**
   * Function to call for part of the processing, this can either be a regular function or a
   * lambda function. This function should consume the produced lambda variables.
   */
  def function: Expression

  /**
   * The number of variables provided by the higher order function. This can be used for
   * validation.
   */
  def numVariables: Int

  /**
   * Bind the lambda variables and function to the [[HigherOrderFunction]]. Lambda variables are
   * guaranteed to be fixed after binding, and should stay fixed until we rebind.
   */
  def bind(names: Seq[String], function: Expression): HigherOrderFunction
}

trait ArrayBasedHigherOrderFunction extends HigherOrderFunction with ExpectsInputTypes {
  def input: Expression

  override def inputs: Seq[Expression] = input :: Nil

  lazy val variable: NamedLambdaVariable = variables.head

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, AnyDataType, AnyDataType)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  protected def createArrayVariable(name: String): NamedLambdaVariable = {
    val ArrayType(elementType, containsNull) = input.dataType
    NamedLambdaVariable(name, elementType, containsNull)
  }

  override def numVariables: Int = 1
}

/**
 * Transform elements in an array using the transform function. This is similar to
 * a `map` in functional programming.
 */
@ExpressionDescription(usage =
  "_FUNC_(expr, func) - Transforms elements in an array using the function.")
case class ArrayTransform(
    input: Expression,
    function: Expression,
    variables: Seq[NamedLambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = input.nullable

  override def dataType: DataType = ArrayType(function.dataType, function.nullable)

  override def bind(names: Seq[String], function: Expression): ArrayTransform = {
    assert(names.size == numVariables)
    ArrayTransform(input, function, createArrayVariable(names.head) :: Nil)
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
 * Tests whether a predicate holds for one or more elements in the array.
 */
@ExpressionDescription(usage =
  "_FUNC_(expr, pred) - Tests whether a predicate holds for one or more elements in the array.")
case class ArrayExists(
    input: Expression,
    function: Expression,
    variables: Seq[NamedLambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = false

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType, AnyDataType)

  override def bind(names: Seq[String], function: Expression): ArrayExists = {
    assert(names.size == numVariables)
    ArrayExists(input, function, createArrayVariable(names.head) :: Nil)
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
 * Filters the input array using the given lambda function.
 */
@ExpressionDescription(usage =
  "_FUNC_(expr, pred) - Filters the input array using the given predicate.")
case class ArrayFilter(
    input: Expression,
    function: Expression,
    variables: Seq[NamedLambdaVariable] = Nil)
  extends ArrayBasedHigherOrderFunction {

  def this(input: Expression, function: Expression) = this(input, function, Nil)

  override def nullable: Boolean = false

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType, AnyDataType)

  override def bind(names: Seq[String], function: Expression): ArrayFilter = {
    assert(names.size == numVariables)
    ArrayFilter(input, function, createArrayVariable(names.head) :: Nil)
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

/**
 * Applies a binary operator to a start value and all elements in the array.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(expr, start, binop) - Applies a binary operator to a start value and all elements in
      the array, and reduces this to a single value.
    """)
case class ArrayReduce(
    input: Expression,
    zero: Expression,
    function: Expression,
    variables: Seq[NamedLambdaVariable] = Nil)
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

  override def bind(names: Seq[String], function: Expression): ArrayReduce = {
    assert(names.size == numVariables)
    val Seq(elementName, accName) = names
    val array = createArrayVariable(elementName)
    val acc = NamedLambdaVariable(accName, zero.dataType, array.nullable || zero.nullable)
    ArrayReduce(input, zero, function, Seq(array, acc))
  }

  override def numVariables: Int = 2

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
    |${zeroGen.code}
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
