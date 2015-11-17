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

package org.apache.spark.sql.catalyst.expressions.codegen.bytecode

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.objectweb.asm.{Label, MethodVisitor, Opcodes}

import scala.collection.mutable

/**
  * A very very experimental backend for direct expression compilation.
  *
  * Things I like to do:
  * - Null check elimination
  * - Common sub-expression elimination
  * - Cache aware access.
  * - Aggressive compilation of opaque expressions.
  * - Play with Row optimizations. If we can 'create' a mono-morphic situation, which is quite
  *   easy then we can make the JIT do more work for us.
  *
  * Things I need to do:
  * - Implement compilation for most expressions.
  * - Implement mutable state.
  * - Implement fallback for projections which exceed the byte code limit of 64K.
  */
object BytecodeEmitter {
  val objectType = name[Object]
  val expressionType = name[Expression]
  val projectionType = name[Projection]
  val generatedClassType = name[GeneratedClass]
  val internalRowType = name[InternalRow]
  val unsafeRowType = name[UnsafeRow]
  val unsafeRowWriterType = name[UnsafeRowWriter]
  val bufferHolderType = name[BufferHolder]

  val classLoader = getClass.getClassLoader

  /** Compile expressions down to bytecode. */
  def generate(expressions: Seq[Expression]): Projection = {
    val (bytecode, references) = compile(expressions)
    new GeneratedClassLoader(classLoader, "SpecificProjection", bytecode)
      .defineClass
      .getConstructors()(0)
      .newInstance(references)
      .asInstanceOf[Projection]
  }

  /** Compile the projection. */
  def compile(expressions: Seq[Expression]): (Array[Byte], Array[Expression]) = {
    // Non-Determinism
    // This would be the place to start optimizing the expression tree.
    val references = mutable.Buffer.empty[Expression]
    val bytecode = create("SpecificProjection", projectionType) { cv =>
      val numFields = expressions.size

      // 'expressions' field.
      cv.visitField("expressions", s"[L$expressionType;")

      // 'result' field
      cv.visitField("result", s"L$unsafeRowType;")

      // 'buffer' field
      cv.visitField("buffer", s"L$bufferHolderType;")

      // 'writer' field
      cv.visitField("writer", s"L$unsafeRowWriterType;")

      // TODO Add mutable state.

      // Create constructor
      val ctor = cv.visitConstructor(s"([L$expressionType;)V")
      ctor.visitVarInsn(Opcodes.ALOAD, 0)
      ctor.visitInsn(Opcodes.DUP)
      ctor.visitInsn(Opcodes.DUP)
      ctor.visitInsn(Opcodes.DUP)
      ctor.visitInsn(Opcodes.DUP)
      ctor.visitConstructorCall(projectionType)
      ctor.visitVarInsn(Opcodes.ALOAD, 1)
      ctor.visitFieldInsn(Opcodes.PUTFIELD,
        "SpecificProjection",
        "expressions",
        s"[L$expressionType;")
      ctor.visitCreateFieldValue("SpecificProjection", "result", unsafeRowType)
      ctor.visitCreateFieldValue("SpecificProjection", "buffer", bufferHolderType)
      ctor.visitCreateFieldValue("SpecificProjection", "writer", unsafeRowWriterType)
      // TODO Initialize mutable state. Take care of the 'this' refs on stack.
      ctor.visitInsn(Opcodes.RETURN)
      ctor.visitMaxsEnd()

      // Create the Function1 'apply' method
      val apply = cv.visitMethod("apply", s"(L$objectType;)L$objectType;")

      // Make sure that we are dealing with an InternalRow
      // TODO skip this?
      apply.visitVarInsn(Opcodes.ALOAD, 1)
      apply.visitTypeInsn(Opcodes.CHECKCAST, internalRowType)
      apply.visitInsn(Opcodes.POP)

      // Reset the buffer & initialize the writer.
      apply.visitVarInsn(Opcodes.ALOAD, 0)
      apply.visitInsn(Opcodes.DUP)
      apply.visitFieldInsn(Opcodes.GETFIELD,
        "SpecificProjection",
        "writer",
        s"L$unsafeRowWriterType;")
      apply.visitInsn(Opcodes.SWAP)
      apply.visitFieldInsn(Opcodes.GETFIELD,
        "SpecificProjection",
        "buffer",
        s"L$bufferHolderType;")
      apply.visitInsn(Opcodes.DUP)
      apply.visitMethodInsn(Opcodes.INVOKEVIRTUAL, bufferHolderType, "reset", "()V")
      apply.visitLdcInsn(numFields)
      apply.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
        unsafeRowWriterType,
        "initialize",
        s"(L$bufferHolderType;I)V")

      // Apply the actual expressions.
      expressions.zipWithIndex.foreach {
        case (expression, i) =>
          apply.visitVarInsn(Opcodes.ALOAD, 0)
          apply.visitFieldInsn(Opcodes.GETFIELD,
            "SpecificProjection",
            "writer",
            s"L$unsafeRowWriterType;")
          apply.visitLdcInsn(i)
          compileExpression(apply, expression, references)



      }

      // Finalize the buffer
      apply.visitVarInsn(Opcodes.ALOAD, 0)
      apply.visitInsn(Opcodes.DUP)
      apply.visitFieldInsn(Opcodes.GETFIELD,
        "SpecificProjection",
        "result",
        s"L$unsafeRowType;")
      apply.visitInsn(Opcodes.DUP_X1)
      apply.visitInsn(Opcodes.SWAP)
      apply.visitFieldInsn(Opcodes.GETFIELD,
        "SpecificProjection",
        "buffer",
        s"L$bufferHolderType;")
      apply.visitInsn(Opcodes.DUP)
      apply.visitFieldInsn(Opcodes.GETFIELD, bufferHolderType, "buffer", "[B")
      apply.visitInsn(Opcodes.SWAP)
      apply.visitMethodInsn(Opcodes.INVOKEVIRTUAL, bufferHolderType, "totalSize", "()I")
      apply.visitLdcInsn(numFields)
      apply.visitInsn(Opcodes.SWAP)
      apply.visitMethodInsn(Opcodes.INVOKEVIRTUAL, unsafeRowType, "pointTo", "([BII)V")
      apply.visitInsn(Opcodes.ARETURN)
      apply.visitMaxsEnd()
    }
    (bytecode, references.toArray)
  }


  private[this] def compileExpression(
      v: MethodVisitor,
      e: Expression,
      references: mutable.Buffer[Expression]): Unit = e match {
    case Literal(null, dataType) =>
      v.visitDefaultValue(dataType)
      v.visitLdcInsn(true)
    case Literal(value, _: NumericType | BooleanType | DateType | TimestampType) =>
      v.visitLdcInsn(value)
    case m: LeafMathExpression =>
      v.visitLdcInsn(m.eval(null))
    case BoundReference(ordinal, dataType, nullable) =>
      v.visitVarInsn(Opcodes.ALOAD, 1)
      if (nullable) {
        v.visitInsn(Opcodes.DUP)
        v.visitLdcInsn(ordinal)
        v.visitMethodInsn(Opcodes.INVOKEVIRTUAL, internalRowType, "isNullAt", "(I)Z")
        val nullBranch = new Label
        val endIf = new Label
        v.visitJumpInsn(Opcodes.IFNE, nullBranch)
        v.visitRowGetter(ordinal, dataType)
        v.visitLdcInsn(false)
        v.visitJumpInsn(Opcodes.GOTO, endIf)
        v.visitLabel(nullBranch)
        v.visitInsn(Opcodes.POP)
        v.visitDefaultValue(dataType)
        v.visitLdcInsn(true)
        v.visitLabel(endIf)
      } else {
        v.visitRowGetter(ordinal, dataType)
      }
    case other =>
      val referenceId = references.size
      references += other
      v.visitVarInsn(Opcodes.ALOAD, 0)
      v.visitFieldInsn(Opcodes.GETFIELD,
        "SpecificProjection",
        "expressions",
        s"[L$expressionType;")
      v.visitLdcInsn(referenceId)
      v.visitInsn(Opcodes.AALOAD)
      v.visitVarInsn(Opcodes.ALOAD, 1)
      v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
        expressionType,
        "eval",
        s"([L$internalRowType;)L$objectType;")
      v.visitInsn(Opcodes.DUP)
      if (other.nullable) {
        val nullBranch = new Label
        val endIf = new Label
        v.visitJumpInsn(Opcodes.IFNULL, nullBranch)
        v.visitUnboxValue(other.dataType)
        v.visitLdcInsn(false)
        v.visitJumpInsn(Opcodes.GOTO, endIf)
        v.visitLabel(nullBranch)
        v.visitInsn(Opcodes.POP)
        v.visitDefaultValue(other.dataType)
        v.visitLdcInsn(true)
        v.visitLabel(endIf)
      } else {
        v.visitUnboxValue(other.dataType)
      }
  }
}

/**
  * [[ClassLoader]] for loading a compiled [[Projection]].
  *
  * @param parent of the current class loader.
  * @param name of the projection class.
  * @param bytecode of the projection class.
  */
private[bytecode] class GeneratedClassLoader(
    parent: ClassLoader,
    name: String,
    bytecode: Array[Byte]) extends ClassLoader(parent) {

  def defineClass: Class[_] =
    defineClass(name, bytecode, 0, bytecode.length, getClass.getProtectionDomain)

  @throws(classOf[ClassNotFoundException])
  protected override def findClass (n: String): Class[_] = {
    if (n == name) {
      defineClass
    } else {
      throw new ClassNotFoundException(name)
    }
  }
}

/** Stolen from [[EquivalentExpressions]]. */
case class Expr(e: Expression) {
  override def equals(o: Any): Boolean = o match {
    case other: Expr => e.semanticEquals(other.e)
    case _ => false
  }
  override val hashCode: Int = e.semanticHash()
}

object IR {
  /**
    * An expression is defined as a Directed Acyclic Graph (DAG) of expressions. A projection is a
    * combination of expressions and is therefore also a DAG.
    *
    *
    * @param expressions
    * @return
    */
  def apply(expressions: Seq[Expression]): AnyRef = {
    def isConditional(e: Expression): Boolean = e match {
      case _: If | _: CaseWhenLike | _: Coalesce | _: AtLeastNNonNulls => true
      case _ => false
    }
    /** Do not deduplicate literals; we can store these in the constant pool. */
    // TODO can we put byte arrays in the constant pool?
    def isPrimitiveLiteral(e: Expression): Boolean = e match {
      case Literal(null, _) => true
      case Literal(_, _: NumericType | BooleanType | DateType | TimestampType) => true
      case _ => true
    }
    // Find duplicate branches.

    //

    // Create subtrees.
    // -
    // -
    // -
    //
    // Find black box expressions.
    // Find non-deterministic expressions.
    // Find
    // Add determinism.
    val references = mutable.Buffer.empty[Expression]
    val exprMap = mutable.Map[Expr, Int]

    // Walk the expression tree for a sequence of expressions.
    def walk(exprs: Seq[Expression], conditional: Boolean): Unit =
      exprs.foreach(walkExpressionTree(_, conditional))

    // Walk the expression tree for a single expression.
    def walkExpressionTree(e: Expression, conditional: Boolean = false): Unit = {
      // Do a depth first traversal of the expression.
      e match {
        case If(predicate, trueValue, falseValue) =>
          walkExpressionTree(predicate, conditional)
          walkExpressionTree(trueValue, true)
          walkExpressionTree(falseValue, true)
        case cwl: CaseWhenLike =>
          val branches = cwl.branches
          walkExpressionTree(branches.head, conditional)
          walk(branches.tail, true)
        case Coalesce(children) =>
          walkExpressionTree(children.head, conditional)
          walk(children.tail, true)
        case AtLeastNNonNulls(n, children) =>
          val (exprs, conditionalExprs) = children.splitAt(n)
          walk(exprs, conditional)
          walk(conditionalExprs, true)
        case _ =>
          walk(e.children, conditional)
      }

      // Add the




    }
    walk(expressions, false)










    null
  }
}