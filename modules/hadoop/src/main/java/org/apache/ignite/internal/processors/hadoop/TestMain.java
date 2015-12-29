package org.apache.ignite.internal.processors.hadoop;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

/**
 * Created by vozerov on 12/28/2015.
 */
public class TestMain {

    public static void main(String[] args) {

        String desc = "(Ljava/lang/String;)Lorg/apache/hadoop/fs/FileSystem;";

        SignatureReader reader = new SignatureReader(desc);

        reader.accept(new SignatureVisitor(Opcodes.ASM4) {
            @Override
            public void visitFormalTypeParameter(String name) {
                System.out.println("visitFormalTypeParameter: " + name);

                super.visitFormalTypeParameter(name);
            }

            @Override
            public SignatureVisitor visitClassBound() {
                return super.visitClassBound();
            }

            @Override
            public SignatureVisitor visitInterfaceBound() {
                return super.visitInterfaceBound();
            }

            @Override
            public SignatureVisitor visitSuperclass() {
                return super.visitSuperclass();
            }

            @Override
            public SignatureVisitor visitInterface() {
                return super.visitInterface();
            }

            @Override
            public SignatureVisitor visitParameterType() {
                return super.visitParameterType();
            }

            @Override
            public SignatureVisitor visitReturnType() {
                return super.visitReturnType();
            }

            @Override
            public SignatureVisitor visitExceptionType() {
                return super.visitExceptionType();
            }

            @Override
            public void visitBaseType(char descriptor) {
                super.visitBaseType(descriptor);
            }

            @Override
            public void visitTypeVariable(String name) {
                System.out.println("visitTypeVariable: " + name);

                super.visitTypeVariable(name);
            }

            @Override
            public SignatureVisitor visitArrayType() {
                return super.visitArrayType();
            }

            @Override
            public void visitClassType(String name) {
                System.out.println("visitClassType: " + name);

                super.visitClassType(name);
            }

            @Override
            public void visitInnerClassType(String name) {
                System.out.println("visitInnerClassType: " + name);

                super.visitInnerClassType(name);
            }

            @Override
            public void visitTypeArgument() {
                super.visitTypeArgument();
            }

            @Override
            public SignatureVisitor visitTypeArgument(char wildcard) {
                System.out.println("visitTypeArgument: " + wildcard);

                return super.visitTypeArgument(wildcard);
            }

            @Override
            public void visitEnd() {
                super.visitEnd();
            }
        });
    }
}
