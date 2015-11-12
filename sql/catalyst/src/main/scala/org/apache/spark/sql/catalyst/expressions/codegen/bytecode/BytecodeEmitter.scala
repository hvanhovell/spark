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
import org.objectweb.asm.{MethodVisitor, Opcodes}

/**
  * A very very experimental backend for direct expression compilation.
  *
  * Things I like to do:
  * - Null check elimination
  * - Common sub-expression elimination
  * - Cache aware access.
  * - Aggressive compilation of opaque expressions.
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

  /** Compile epxressions down to bytecode. */
  def compile(expressions: Seq[Expression]): GeneratedClass = {
    val projection = createProjection(expressions)
    val loader = new GeneratedClassLoader(classLoader, Seq(generator, projection))
    loader.defineClass.newInstance().asInstanceOf[GeneratedClass]
  }

  /** Byte code for the generator class. */
  private[this] val generator = create("SpecificGeneratedClass", generatedClassType) { cv =>
    // Constructor
    val ctr = cv.visitConstructor()
    ctr.visitVarInsn(Opcodes.ALOAD, 0)
    ctr.visitConstructorCall(generatedClassType)
    ctr.visitInsn(Opcodes.RETURN)
    ctr.visitMaxsEnd()

    // Generate method
    val gen = cv.visitMethod("generate", s"([L$expressionType;)L$objectType;")
    gen.visitTypeInsn(Opcodes.NEW, "SpecificProjection")
    gen.visitInsn(Opcodes.DUP)
    gen.visitVarInsn(Opcodes.ALOAD, 1)
    gen.visitConstructorCall("SpecificProjection", s"([L$expressionType;)V")
    gen.visitInsn(Opcodes.ARETURN)
    gen.visitMaxsEnd()
  }

  /** Create a projection body. */
  private[this] def createProjection(expressions: Seq[Expression]): (String, Array[Byte]) =
    create("SpecificProjection", projectionType) { cv =>
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
      expressions.foreach(compileExpression(apply, _))

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

  private[this] def compileExpression(v: MethodVisitor, e: Expression): Unit =
    v.visitInsn(Opcodes.NOP)
}

/**
  * [[ClassLoader]] for loading a [[GeneratedClass]] and all the other classes it requires.
  *
  * @param classes contains the class definitions.
  * @param parent of the current class loader.
  */
private[bytecode] class GeneratedClassLoader(
    parent: ClassLoader,
    private[this] val classes: Seq[(String, Array[Byte])]) extends ClassLoader(parent) {
  def defineClass: Class[_] = defineClass(classes.head)

  private[this] def defineClass(definition: (String, Array[Byte])): Class[_] = {
    val (name, bytes) = definition
    defineClass(name, bytes, 0, bytes.length, getClass.getProtectionDomain)
  }

  @throws(classOf[ClassNotFoundException])
  protected override def findClass (name: String): Class[_] = {
    classes.find(_._1 == name) match {
      case Some(definition) =>
        defineClass(definition)
      case None =>
        throw new ClassNotFoundException(name)
    }
  }
}