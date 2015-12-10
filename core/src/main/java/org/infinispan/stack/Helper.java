package org.infinispan.stack;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtPrimitiveType;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Helper {
   static final CtClass[] EMPTY_PARAMS = new CtClass[0];
   static final String DUMP_CLASSES = System.getProperty("infinispan.stack.dump.classes");

   private static final Log log = LogFactory.getLog(Helper.class);
   private static final Helper DUMMY = new Helper();
   static final String CLASSNAME = Helper.class.getName();

   private Helper() {}

   /**
    * This method is used from generated class initializers to access private static fields
    *
    * @param className
    * @param fieldName
    * @return
    */
   public static Object readPrivate(String className, String fieldName) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
      return readPrivate(null, className, fieldName);
   }

   /**
    * This method is used from generated class initializers to access private non-static fields
    *
    * @param className
    * @param fieldName
    * @return
    */
   public static Object readPrivate(Object instance, String className, String fieldName) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
      Class<?> clazz = Class.forName(className);
      Field f = clazz.getDeclaredField(fieldName);
      f.setAccessible(true);
      return f.get(instance);
   }

   /**
    * This method is used from generated class initializers to create a copy of given instance
    * as the rewritten class, and based on the actual instance (not just from static type info).
    *
    * @param instance
    * @param interceptor
    * @param table
    * @return
    * @throws ClassNotFoundException
    * @throws NoSuchMethodException
    * @throws IllegalAccessException
    * @throws InvocationTargetException
    * @throws InstantiationException
    */
   public static Object dynamicCopy(Object instance, Object interceptor, String[] table) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
      if (instance == null) return null;
      String className = instance.getClass().getName();
      for (int i = 0; i < table.length; i += 2) {
         if (className.equals(table[i])) {
            String newClassName = table[i + 1];
            for (Constructor ctor : Class.forName(newClassName).getDeclaredConstructors()) {
               Class[] parameterTypes = ctor.getParameterTypes();
               if (parameterTypes.length == 3 && parameterTypes[2].equals(Helper.class)) {
                  return ctor.newInstance(instance, interceptor, DUMMY);
               }
            }
            StringBuilder sb = new StringBuilder("Cannot find proper ctor for ").append(newClassName).append(", available are:\n");
            for (Constructor ctor : Class.forName(newClassName).getDeclaredConstructors()) {
               sb.append(ctor.toString()).append('\n');
            }
            throw new IllegalArgumentException(sb.toString());
         }
      }
      StringBuilder sb = new StringBuilder().append("Unknown replaced class ").append(className).append(", available are:\n");
      for (int i = 0; i < table.length; i += 2) {
         sb.append(table[i]).append(" -> ").append(table[i + 1]).append('\n');
      }
      throw new IllegalArgumentException(sb.toString());
   }

   static void printInstructions(MethodInfo methodInfo, ConstPool constPool, CodeAttribute codeAttribute) throws BadBytecode {
      CodeIterator iterator = codeAttribute.iterator();
      while (iterator.hasNext()) {
         int index = iterator.next();
         printInstruction(methodInfo, constPool, iterator, index);
      }
      ExceptionTable exceptionTable = codeAttribute.getExceptionTable();
      if (exceptionTable.size() > 0) {
         log.trace("Exception table: ");
      }
      for (int i = 0; i < exceptionTable.size(); ++i) {
         log.tracef("%3d - %3d -> %3d | %3d = %s\n",
               exceptionTable.startPc(i), exceptionTable.endPc(i),
               exceptionTable.handlerPc(i), exceptionTable.catchType(i),
               constPool.getClassInfo(exceptionTable.catchType(i)));
      }
   }

   static void printInstruction(MethodInfo methodInfo, ConstPool constPool, CodeIterator iterator, int index) {
      int op = iterator.byteAt(index);
      String attributes;
      int methodId, branchOffset, tableStart, defaultOffset, numLabels;
      StringBuilder sb;
      switch (op) {
         case Opcode.GOTO_W:
         case Opcode.JSR_W:
            branchOffset = iterator.s32bitAt(index + 1);
            attributes = String.format("-> %+3d = %3d", branchOffset, index + branchOffset);
            break;
         case Opcode.NEW:
            attributes = constPool.getClassInfo(iterator.u16bitAt(index + 1));
            break;
         case Opcode.GOTO:
         case Opcode.JSR:
         case Opcode.IFGE:
         case Opcode.IFGT:
         case Opcode.IFLE:
         case Opcode.IFLT:
         case Opcode.IFNE:
         case Opcode.IFNONNULL:
         case Opcode.IFNULL:
         case Opcode.IF_ACMPEQ:
         case Opcode.IF_ACMPNE:
         case Opcode.IF_ICMPEQ:
         case Opcode.IF_ICMPGE:
         case Opcode.IF_ICMPGT:
         case Opcode.IF_ICMPLE:
         case Opcode.IF_ICMPLT:
         case Opcode.IF_ICMPNE:
            branchOffset = iterator.u16bitAt(index + 1);
            attributes = String.format("-> %+3d = %d", branchOffset, index + branchOffset);
            break;
         case Opcode.LOOKUPSWITCH:
            tableStart = (index & ~3) + 4;
            defaultOffset = iterator.s32bitAt(tableStart);
            numLabels = iterator.s32bitAt(tableStart + 4);
            sb = new StringBuilder().append(numLabels).append(" labels\n");
            for (int i = 0; i < numLabels; ++i) {
               int key = iterator.s32bitAt(tableStart + 8 + 8*i);
               branchOffset = iterator.s32bitAt(tableStart + 12 + 8*i);
               sb.append(String.format("\t\t%3d -> %3+d = %3d\n", key, branchOffset, index + branchOffset));
            }
            sb.append("\t\tdef\t-> ").append(defaultOffset);
            attributes = sb.toString();
            break;
         case Opcode.TABLESWITCH:
            tableStart = (index & ~3) + 4;
            defaultOffset = iterator.s32bitAt(tableStart);
            int low = iterator.s32bitAt(tableStart + 4);
            int high = iterator.s32bitAt(tableStart + 8);
            numLabels = high - low + 1;
            sb = new StringBuilder().append(low).append(" - ").append(high).append("\n");
            for (int i = 0; i < numLabels; ++i) {
               branchOffset = iterator.s32bitAt(tableStart + 12 + 4*i);
               sb.append(String.format("\t\t%3d -> %+3d = %3d\n", low + i, branchOffset, index + branchOffset));
            }
            sb.append(String.format("\t\tdef\t-> %+3d = %3d", defaultOffset, index + defaultOffset));
            attributes = sb.toString();
            break;
         case Opcode.INVOKEVIRTUAL:
         case Opcode.INVOKESPECIAL:
         case Opcode.INVOKESTATIC:
            methodId = iterator.u16bitAt(index + 1);
            attributes = constPool.getMethodrefClassName(methodId) + "." + constPool.getMethodrefName(methodId) + constPool.getMethodrefType(methodId);
            break;
         case Opcode.INVOKEINTERFACE :
            methodId = iterator.u16bitAt(index + 1);
            attributes = constPool.getInterfaceMethodrefClassName(methodId) + "." + constPool.getInterfaceMethodrefName(methodId) + constPool.getInterfaceMethodrefType(methodId);
            break;
         case Opcode.INVOKEDYNAMIC :
            // don't know how to handle this
            methodId = iterator.u16bitAt(index + 1);
            attributes = constPool.getInvokeDynamicType(methodId);
            break;
         case Opcode.LDC:
            attributes = String.valueOf(constPool.getLdcValue(iterator.byteAt(index + 1)));
            break;
         case Opcode.LDC_W:
         case Opcode.LDC2_W:
            attributes = String.valueOf(constPool.getLdcValue(iterator.u16bitAt(index + 1)));
            break;
         case Opcode.ALOAD:
         case Opcode.ASTORE:
            attributes = String.valueOf(iterator.byteAt(index + 1));
            break;
         default:
            attributes = "";
      }
      log.tracef("[line %2d] %3d: %s %s\n", methodInfo.getLineNumber(index), index, Mnemonic.OPCODE[op], attributes);
   }

   static void addCast(Bytecode bytecode, CtClass type) throws NotFoundException {
      if (type.isPrimitive()) {
         CtPrimitiveType primitiveType = (CtPrimitiveType) type;
         bytecode.addCheckcast(primitiveType.getWrapperName());
         bytecode.addInvokevirtual(primitiveType.getWrapperName(), primitiveType.getGetMethodName(), primitiveType.getGetMethodDescriptor());
      } else {
         bytecode.addCheckcast(type);
      }
   }

   /*static String prefixedName(String newName, CtClass declaringClass, CtClass topClass) {
      if (!declaringClass.equals(topClass)) {
         if (declaringClass.getPackageName().equals(topClass.getPackageName())) {
            newName = declaringClass.getSimpleName() + "$" + newName;
         } else {
            newName = declaringClass.getName().replaceAll("\\.", "\\$") + "$" + newName;
         }
      }
      return newName;
   }*/

   static CtClass[] replaceInParams(CtClass[] parameterTypes, Map<String, String> replacedClassNames, ClassPool classPool) throws NotFoundException {
      CtClass[] newParams = new CtClass[parameterTypes.length];
      for (int i = 0; i < parameterTypes.length; ++i) {
         String newType = replacedClassNames.get(parameterTypes[i].getName());
         if (newType != null) {
            newParams[i] = classPool.get(newType);
         } else {
            newParams[i] = parameterTypes[i];
         }
      }
      return newParams;
   }

   static int[] copyBootstrapMethodArguments(ClassPool classPool, ConstPool oldConstPool, ConstPool newConstPool, BootstrapMethodsAttribute.BootstrapMethod bootstrapMethod, Map<String, String> replacedClassNames, MethodCopier methodCopier) throws NotFoundException {
      int[] newBootstrapMethodArguments = new int[bootstrapMethod.arguments.length];
      for (int i = 0; i < bootstrapMethod.arguments.length; ++i) {
         int argument = bootstrapMethod.arguments[i];
         switch (oldConstPool.getTag(argument)) {
            case Constants.STRING_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addStringInfo(oldConstPool.getStringInfo(argument));
               break;
            case Constants.CLASS_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addClassInfo(oldConstPool.getClassInfo(argument));
               break;
            case Constants.INTEGER_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addIntegerInfo(oldConstPool.getIntegerInfo(argument));
               break;
            case Constants.LONG_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addLongInfo(oldConstPool.getLongInfo(argument));
               break;
            case Constants.FLOAT_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addFloatInfo(oldConstPool.getFloatInfo(argument));
               break;
            case Constants.DOUBLE_INFO:
               newBootstrapMethodArguments[i] = newConstPool.addDoubleInfo(oldConstPool.getDoubleInfo(argument));
               break;
            case Constants.METHOD_HANDLE_INFO:
               newBootstrapMethodArguments[i] = copyMethodHandle(classPool, oldConstPool, newConstPool, argument, replacedClassNames, methodCopier);
               break;
            case Constants.METHOD_TYPE_INFO:
               newBootstrapMethodArguments[i] = copyMethodType(oldConstPool, newConstPool, argument);
               break;
         }
      }
      return newBootstrapMethodArguments;
   }

   private static int copyMethodType(ConstPool oldConstPool, ConstPool newConstPool, int methodTypeIndex) {
      int descriptorIndex = oldConstPool.getMethodTypeInfo(methodTypeIndex);
      int newDescriptorIndex = newConstPool.addUtf8Info(oldConstPool.getUtf8Info(descriptorIndex));
      return newConstPool.addMethodTypeInfo(newDescriptorIndex);
   }

   static CtClass[] prependClass(CtClass newClass, CtClass[] parameterTypes) {
      CtClass[] newParameterTypes = new CtClass[parameterTypes.length + 1];
      newParameterTypes[0] = newClass;
      System.arraycopy(parameterTypes, 0, newParameterTypes, 1, parameterTypes.length);
      return newParameterTypes;
   }

   static void addCopyField(Bytecode bytecode, CtField srcField, CtField destField, CtClass newInterceptor, int interceptorVar, CtClass baseInterceptor) throws NotFoundException {
      int modifiers = srcField.getModifiers();
      boolean destIsInterceptor = newInterceptor != null ? destField.getType().getName().equals(newInterceptor.getName()) : false;
      boolean useDynamicCopy = !destIsInterceptor && !srcField.getType().getName().equals(destField.getType().getName());
      if (Modifier.isStatic(modifiers)) {
         if (isAccessible(srcField, destField.getDeclaringClass())) {
            bytecode.addGetstatic(srcField.getDeclaringClass(), srcField.getName(), srcField.getSignature());
         } else {
            // we need to generate reflexive call
            bytecode.addLdc(srcField.getDeclaringClass().getName());
            bytecode.addLdc(srcField.getName());
            bytecode.addInvokestatic(Helper.CLASSNAME, "readPrivate", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/Object;");
            if (!useDynamicCopy) {
               addCast(bytecode, srcField.getType());
            }
         }
      } else if (destIsInterceptor) {
         bytecode.addAload(0);
         // to copy constructors the interceptor is passed as second argument
         if (interceptorVar < 0) {
            bytecode.addConstZero(newInterceptor);
         } else {
            bytecode.addAload(interceptorVar);
         }
         bytecode.addCheckcast(newInterceptor);
      } else {
         bytecode.addAload(0);
         // copied instance is passed as first argument
         bytecode.addAload(1);
         if (isAccessible(srcField, destField.getDeclaringClass())) {
            bytecode.addGetfield(srcField.getDeclaringClass(), srcField.getName(), srcField.getSignature());
         } else {
            bytecode.addLdc(srcField.getDeclaringClass().getName());
            bytecode.addLdc(srcField.getName());
            bytecode.addInvokestatic(Helper.CLASSNAME, "readPrivate", "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/Object;");
            if (!useDynamicCopy) {
               addCast(bytecode, srcField.getType());
            }
         }
      }
      if (useDynamicCopy) {
         // we need unique parameter to create our special copy constructor
         if (interceptorVar < 0) {
            bytecode.addOpcode(Opcode.ACONST_NULL);
         } else {
            bytecode.addAload(interceptorVar);
         }
         bytecode.addGetstatic(baseInterceptor, InterceptorGenerator.REPLACE_TABLE, InterceptorGenerator.STRING_ARRAY_DESCRIPTOR);
         bytecode.addInvokestatic(Helper.CLASSNAME, "dynamicCopy", "(Ljava/lang/Object;Ljava/lang/Object;[Ljava/lang/String;)Ljava/lang/Object;");
         bytecode.addCheckcast(destField.getType());
      }
      if (Modifier.isStatic(modifiers)) {
         bytecode.addPutstatic(destField.getDeclaringClass(), destField.getName(), destField.getSignature());
      } else {
         bytecode.addPutfield(destField.getDeclaringClass(), destField.getName(), destField.getSignature());
      }
   }

   private static boolean isAccessible(CtField field, CtClass clazz) {
      int modifiers = field.getModifiers();
      if (Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers)) return false;
      if (Modifier.isPublic(modifiers)) return true;
      return clazz.getPackageName().equals(field.getDeclaringClass().getPackageName());
   }

   // putfield/getfield can reference subtype of the actually declaring class
   static CtClass getFieldDeclaringClass(CtClass clazz, String fieldName) throws NotFoundException {
      while (clazz != null) {
         for (CtField f : clazz.getDeclaredFields()) {
            if (f.getName().equals(fieldName)) {
               return clazz;
            }
         }
         clazz = clazz.getSuperclass();
      }
      throw new NotFoundException(fieldName);
   }

   static CtClass getMethodDeclaringClass(CtClass clazz, String methodName, String signature) throws NotFoundException {
      while (clazz != null) {
         for (CtMethod m : clazz.getDeclaredMethods()) {
            if (m.getName().equals(methodName) && m.getSignature().equals(signature)) {
               return clazz;
            }
         }
         clazz = clazz.getSuperclass();
      }
      throw new NotFoundException(methodName + "(" + signature + ")");
   }

   public static void removeAttribute(List<AttributeInfo> attributes, String tag) {
      for (Iterator<AttributeInfo> it = attributes.iterator(); it.hasNext(); ) {
         if (it.next().getName().equals(tag)) {
            it.remove();
         }
      }
   }

   public static boolean containsMethod(CtClass c, String methodName, CtClass[] parameterTypes) throws NotFoundException {
      if (c == null) return false;
      for (CtMethod m : c.getDeclaredMethods()) {
         if (m.getName().equals(methodName) && Arrays.equals(m.getParameterTypes(), parameterTypes)) {
            return true;
         }
      }
      return false;
   }

   public static CtClass getOuterInterceptor(CtClass innerClass, Map<String, CtClass> newInterceptorsByOldName) throws NotFoundException {
      for (;;) {
         CtClass declaringClass = innerClass.getDeclaringClass();
         if (declaringClass == null) {
            return null;
         }
         CtClass interceptor = newInterceptorsByOldName.get(declaringClass.getName());
         if (interceptor != null) {
            return interceptor;
         }
         innerClass = declaringClass;
      }
   }

   interface MethodCopier {
      NameAndDescriptor get(CtMethod method) throws NotFoundException;
   }

   static int copyMethodHandle(ClassPool classPool, ConstPool oldConstPool, ConstPool newConstPool, int methodHandleIndex, Map<String, String> replacedClassNames, MethodCopier methodCopier) throws NotFoundException {
      int methodHandleKind = oldConstPool.getMethodHandleKind(methodHandleIndex);
      int methodIndex = oldConstPool.getMethodHandleIndex(methodHandleIndex);
      String className, methodName, methodSignature;
      switch (methodHandleKind) {
         case ConstPool.REF_invokeSpecial:
         case ConstPool.REF_invokeStatic:
         case ConstPool.REF_invokeVirtual:
            className = oldConstPool.getMethodrefClassName(methodIndex);
            methodName = oldConstPool.getMethodrefName(methodIndex);
            methodSignature = oldConstPool.getMethodrefType(methodIndex);
            break;
         case ConstPool.REF_invokeInterface:
            className = oldConstPool.getInterfaceMethodrefClassName(methodIndex);
            methodName = oldConstPool.getInterfaceMethodrefName(methodIndex);
            methodSignature = oldConstPool.getInterfaceMethodrefType(methodIndex);
            break;
         default:
            throw new IllegalArgumentException("Unexpected");
      }
      if (replacedClassNames.containsKey(className)) {
         CtClass methodClass = classPool.get(className);
         CtMethod method = methodClass.getDeclaredMethod(methodName, Descriptor.getParameterTypes(methodSignature, classPool));
         NameAndDescriptor copy = methodCopier.get(method);
         className = copy.getClassName();
         methodName = copy.getName();
         methodSignature = copy.getDescriptor();
      }
      int newMethodIndex;
      switch (methodHandleKind) {
         case ConstPool.REF_invokeSpecial:
         case ConstPool.REF_invokeStatic:
         case ConstPool.REF_invokeVirtual:
            newMethodIndex = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(className), methodName, methodSignature);
            break;
         case ConstPool.REF_invokeInterface:
            newMethodIndex = newConstPool.addInterfaceMethodrefInfo(newConstPool.addClassInfo(className), methodName, methodSignature);
            break;
         default:
            throw new IllegalArgumentException("Unexpected");
      }
      return newConstPool.addMethodHandleInfo(methodHandleKind, newMethodIndex);
   }

   static String replaceInDescriptor(ClassPool classPool, Map<String, String> replacedClassNames, String descriptor) throws NotFoundException {
      CtClass[] parameterTypes = Descriptor.getParameterTypes(descriptor, classPool);
      for (int i = 0; i < parameterTypes.length; ++i) {
         parameterTypes[i] = replacedClass(classPool, replacedClassNames, parameterTypes[i]);
      }
      return Descriptor.ofMethod(replacedClass(classPool, replacedClassNames, Descriptor.getReturnType(descriptor, classPool)), parameterTypes);
   }

   static CtClass replacedClass(ClassPool classPool, Map<String, String> replacedClassNames, CtClass clazz) throws NotFoundException {
      String replaced = replacedClassNames.get(clazz.getName());
      return replaced != null ? classPool.get(replaced) : clazz;
   }
}
