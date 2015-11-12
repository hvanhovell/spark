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

package org.apache.spark.sql.catalyst.expressions.codegen
import org.objectweb.asm._

import scala.reflect.ClassTag

package object bytecode {
  /** Always create public final classes. */
  val CLASS_ACCESS_FLAGS = Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER + Opcodes.ACC_FINAL

  /** Always create private final fields. */
  val FIELD_ACCESS_FLAGS = Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL

  /** Get the internal name of a class. */
  def name[E](implicit e: ClassTag[E]): String = Type.getInternalName(e.runtimeClass)

  /** Create a class. */
  def create(name: String, superClass: String)(f: ClassVisitor => Unit): (String, Array[Byte]) = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS)
    cw.visit(Opcodes.V1_6, CLASS_ACCESS_FLAGS, name, null, superClass, Array.empty)
    f(cw)
    (name, cw.toByteArray)
  }

  /** Helper methods for the class visitor. */
  implicit  class ClassVisitorExt(val cv: ClassVisitor) extends AnyVal {
    def visitMethod(name: String, desc: String): MethodVisitor = {
      val mv = cv.visitMethod(Opcodes.ACC_PUBLIC, name, desc, null, Array.empty)
      mv.visitCode()
      mv
    }

    def visitConstructor(desc: String = "()V"): MethodVisitor =
      visitMethod("<init>", desc)

    def visitField(name: String, desc: String): Unit =
      cv.visitField(FIELD_ACCESS_FLAGS, name, desc, null, null).visitEnd()
  }

  /** Helper methods for the method visitor. */
  implicit class MethodVistorExt(val mv: MethodVisitor) extends AnyVal {
    def visitConstructorCall(owner: String, desc: String = "()V"): Unit =
      mv.visitMethodInsn(Opcodes.INVOKESPECIAL, owner, "<init>", desc)

    def visitMaxsEnd(): Unit = {
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    def visitCreateFieldValue(owner: String, name: String, tpe: String): Unit = {
      mv.visitTypeInsn(Opcodes.NEW, tpe)
      mv.visitInsn(Opcodes.DUP)
      mv.visitConstructorCall(tpe)
      mv.visitFieldInsn(Opcodes.PUTFIELD, owner, name, s"L$tpe;")
    }
  }
}
